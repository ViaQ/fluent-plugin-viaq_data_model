#
# Fluentd ViaQ data model Filter Plugin
#
# Copyright 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require 'time'
require 'date'

require 'fluent/filter'
require 'fluent/log'
require 'fluent/match'

begin
  ViaqMatchClass = Fluent::Match
rescue
  # Fluent::Match not provided with 0.14
  class ViaqMatchClass
    def initialize(pattern_str, unused)
      patterns = pattern_str.split(/\s+/).map {|str|
        Fluent::MatchPattern.create(str)
      }
      if patterns.length == 1
        @pattern = patterns[0]
      else
        @pattern = Fluent::OrMatchPattern.new(patterns)
      end
    end
    def match(tag)
      if @pattern.match(tag)
        return true
      end
      return false
    end
    def to_s
      "#{@pattern}"
    end
  end
end

module Fluent
  class ViaqDataModelFilter < Filter
    Fluent::Plugin.register_filter('viaq_data_model', self)

    desc 'Default list of comma-delimited fields to keep in each record'
    config_param :default_keep_fields, default: [] do |val|
      val.split(',')
    end

    desc 'Optional extra list of comma-delimited fields to keep in each record'
    config_param :extra_keep_fields, default: [] do |val|
      val.split(',')
    end

    # The kibana pod emits log records with an empty message field
    # we want to preserve these empty messages
    desc 'List of fields to keep as empty fields - also added to extra_keep_fields'
    config_param :keep_empty_fields, default: ['message'] do |val|
      val.split(',')
    end

    desc 'Use "undefined" field to store fields not in above lists'
    config_param :use_undefined, :bool, default: false

    desc 'Name of undefined field to store fields not in above lists if use_undefined is true'
    config_param :undefined_name, :string, default: 'undefined'

    # we can't directly add a field called @timestamp in a record_transform
    # filter because the '@' is special to fluentd
    desc 'Rename timestamp field to Elasticsearch compatible name'
    config_param :rename_time, :bool, default: true

    desc 'Rename timestamp field to Elasticsearch compatible name only if the destination field does not already exist'
    config_param :rename_time_if_missing, :bool, default: false

    desc 'Name of source timestamp field'
    config_param :src_time_name, :string, default: 'time'

    desc 'Name of destination timestamp field'
    config_param :dest_time_name, :string, default: '@timestamp'

    # <formatter>
    #   type sys_journal
    #   tag "journal.system**"
    #   remove_keys log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID
    # </formatter>
    # formatters will be processed in the order specified, so make sure more specific matches
    # come before more general matches
    desc 'Formatters for common data model, for well known record types'
    config_section :formatter, param_name: :formatters do
      desc 'one of the well known formatter types'
      config_param :type, :enum, list: [:sys_journal, :k8s_journal, :sys_var_log, :k8s_json_file]
      desc 'process records with this tag pattern'
      config_param :tag, :string
      desc 'remove these keys from the record - same as record_transformer "remove_keys" field'
      config_param :remove_keys, :string, default: nil
    end

    desc 'Which part of the pipeline is this - collector, normalizer, etc. for pipeline_metadata'
    config_param :pipeline_type, :enum, list: [:collector, :normalizer], default: :collector

    # e.g.
    # <elasticsearch_index_name>
    #   tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
    #   code "begin '.operations.' + (record['@timestamp'].nil? ? Time.at(time).getutc.strftime('%Y.%m.%d') : Time.parse(record['@timestamp']).getutc.strftime('%Y.%m.%d')) rescue $log.error('record is missing time and @timestamp - record ' + record.to_s) end"
    # </elasticsearch_index_name>
    # <elasticsearch_index_name>
    #   tag "**"
    #   code "if record['kubernetes'].nil?; $log.error('record is missing kubernetes field ' + record.to_s); elsif record['kubernetes']['namespace_name'].nil?; $log.error('record is missing kubernetes.namespace_name field ' + record.to_s); elsif record['kubernetes']['namespace_id'].nil?; $log.error('record is missing kubernetes.namespace_id field ' + record.to_s); elsif (record['@timestamp'].nil? && record['time'].nil?); $log.error('record is missing @timestamp and time fields ' + record.to_s); else 'project.' + record['kubernetes']['namespace_name'] + '.' + record['kubernetes']['namespace_id'] + '.' + (record['@timestamp'].nil? ? Time.at(time).getutc.strftime('%Y.%m.%d') : Time.parse(record['@timestamp']).getutc.strftime('%Y.%m.%d')) end"
    # </elasticsearch_index_name>
    # the code will be wrapped as a method like this:
    # def get_index_name(record) ... your code goes here ... end
    # and the code must return a string
    desc 'Construct Elasticsearch index names using given code based on the matching tags pattern'
    config_section :elasticsearch_index_name, param_name: :elasticsearch_index_names do
      config_param :tag, :string
      config_param :code, :string
    end
    desc 'Store the Elasticsearch index name in this field'
    config_param :elasticsearch_index_field, :string, default: nil

    def configure(conf)
      super
      @keep_fields = {}
      @default_keep_fields.each{|xx| @keep_fields[xx] = true}
      @extra_keep_fields.each{|xx| @keep_fields[xx] = true}
      @keep_empty_fields_hash = {}
      @keep_empty_fields.each do |xx|
        @keep_empty_fields_hash[xx] = true
        @keep_fields[xx] = true
      end
      if @use_undefined && @keep_fields.key?(@undefined_name)
        raise Fluent::ConfigError, "Do not put [#{@undefined_name}] in default_keep_fields or extra_keep_fields"
      end
      if (@rename_time || @rename_time_if_not_exist) && @use_undefined && !@keep_fields.key?(@src_time_name)
        raise Fluent::ConfigError, "Field [#{@src_time_name}] must be listed in default_keep_fields or extra_keep_fields"
      end
      if @formatters
        @formatters.each do |fmtr|
          matcher = ViaqMatchClass.new(fmtr.tag, nil)
          fmtr.instance_eval{ @params[:matcher] = matcher }
          fmtr.instance_eval{ @params[:fmtr_type] = fmtr.type }
          if fmtr.remove_keys
            fmtr.instance_eval{ @params[:fmtr_remove_keys] = fmtr.remove_keys.split(',') }
          else
            fmtr.instance_eval{ @params[:fmtr_remove_keys] = nil }
          end
          if fmtr.type == :sys_journal || fmtr.type == :k8s_journal
            fmtr_func = method(:process_journal_fields)
          elsif fmtr.type == :sys_var_log
            fmtr_func = method(:process_sys_var_log_fields)
          elsif fmtr.type == :k8s_json_file
            fmtr_func = method(:process_k8s_json_file_fields)
          end
          fmtr.instance_eval{ @params[:fmtr_func] = fmtr_func }
        end
        @formatter_cache = {}
        @formatter_cache_nomatch = {}
      end
      begin
        @docker_hostname = File.open('/etc/docker-hostname') { |f| f.readline }.rstrip
      rescue
        @docker_hostname = nil
      end
      @ipaddr4 = ENV['IPADDR4'] || '127.0.0.1'
      @ipaddr6 = ENV['IPADDR6'] || '::1'
      @pipeline_version = (ENV['FLUENTD_VERSION'] || 'unknown fluentd version') + ' ' + (ENV['DATA_VERSION'] || 'unknown data version')
      if @elasticsearch_index_field && @elasticsearch_index_names.empty?
        raise Fluent::ConfigError, "Field elasticsearch_index_field specified but no elasticsearch_index_name values were configured"
      end
      if !@elasticsearch_index_names.empty? && !@elasticsearch_index_field
        raise Fluent::ConfigError, "Field elasticsearch_index_name specified but no elasticsearch_index_field value was configured"
      end
      # compile the code
      if !@elasticsearch_index_names.empty? && @elasticsearch_index_field
        @elasticsearch_index_names.each do |ein|
          func = eval('lambda{|record| ' + ein.code + '}')
          ein.instance_eval{ @params[:func] = func }
          matcher = ViaqMatchClass.new(ein.tag, nil)
          ein.instance_eval{ @params[:matcher] = matcher }
        end
        # test the code
        @elasticsearch_index_names.each{|ein| ein.func([])}
      end
    end

    def start
      super
    end

    def shutdown
      super
    end

    # if thing doesn't respond to empty? then assume it isn't empty e.g.
    # 0.respond_to?(:empty?) == false - the FixNum 0 is not empty
    def isempty(thing)
      thing.respond_to?(:empty?) && thing.empty?
    end

    # recursively delete empty fields and empty lists/hashes from thing
    def delempty(thing)
      if thing.respond_to?(:delete_if)
        if thing.kind_of? Hash
          thing.delete_if{|k,v| v.nil? || isempty(delempty(v)) || isempty(v)}
        else # assume single element iterable
          thing.delete_if{|elem| elem.nil? || isempty(delempty(elem)) || isempty(elem)}
        end
      end
      thing
    end

    # map of journal fields to viaq data model field
    JOURNAL_FIELD_MAP_SYSTEMD_T = {
      "_AUDIT_LOGINUID"    => "AUDIT_LOGINUID",
      "_AUDIT_SESSION"     => "AUDIT_SESSION",
      "_BOOT_ID"           => "BOOT_ID",
      "_CAP_EFFECTIVE"     => "CAP_EFFECTIVE",
      "_CMDLINE"           => "CMDLINE",
      "_COMM"              => "COMM",
      "_EXE"               => "EXE",
      "_GID"               => "GID",
      "_MACHINE_ID"        => "MACHINE_ID",
      "_PID"               => "PID",
      "_SELINUX_CONTEXT"   => "SELINUX_CONTEXT",
      "_SYSTEMD_CGROUP"    => "SYSTEMD_CGROUP",
      "_SYSTEMD_OWNER_UID" => "SYSTEMD_OWNER_UID",
      "_SYSTEMD_SESSION"   => "SYSTEMD_SESSION",
      "_SYSTEMD_SLICE"     => "SYSTEMD_SLICE",
      "_SYSTEMD_UNIT"      => "SYSTEMD_UNIT",
      "_SYSTEMD_USER_UNIT" => "SYSTEMD_USER_UNIT",
      "_TRANSPORT"         => "TRANSPORT",
      "_UID"               => "UID"
    }

    JOURNAL_FIELD_MAP_SYSTEMD_U = {
      "CODE_FILE"         => "CODE_FILE",
      "CODE_FUNCTION"     => "CODE_FUNCTION",
      "CODE_LINE"         => "CODE_LINE",
      "ERRNO"             => "ERRNO",
      "MESSAGE_ID"        => "MESSAGE_ID",
      "RESULT"            => "RESULT",
      "UNIT"              => "UNIT",
      "SYSLOG_FACILITY"   => "SYSLOG_FACILITY",
      "SYSLOG_IDENTIFIER" => "SYSLOG_IDENTIFIER",
      "SYSLOG_PID"        => "SYSLOG_PID"
    }

    JOURNAL_FIELD_MAP_SYSTEMD_K = {
      "_KERNEL_DEVICE"    => "KERNEL_DEVICE",
      "_KERNEL_SUBSYSTEM" => "KERNEL_SUBSYSTEM",
      "_UDEV_SYSNAME"     => "UDEV_SYSNAME",
      "_UDEV_DEVNODE"     => "UDEV_DEVNODE",
      "_UDEV_DEVLINK"     => "UDEV_DEVLINK",
    }

    JOURNAL_TIME_FIELDS = ['_SOURCE_REALTIME_TIMESTAMP', '__REALTIME_TIMESTAMP']

    def process_journal_fields(tag, time, record, fmtr_type)
      reckeys = record.keys
      reckeys.each do |jkey|
        if key = JOURNAL_FIELD_MAP_SYSTEMD_T[jkey]
          ((record['systemd'] ||= {})['t'] ||= {})[key] = record[jkey]
        elsif key = JOURNAL_FIELD_MAP_SYSTEMD_U[jkey]
          ((record['systemd'] ||= {})['u'] ||= {})[key] = record[jkey]
        elsif key = JOURNAL_FIELD_MAP_SYSTEMD_K[jkey]
          ((record['systemd'] ||= {})['k'] ||= {})[key] = record[jkey]
        end
      end
      begin
        pri_index = ('%d' % record['PRIORITY'] || 9).to_i
        if pri_index < 0
          pri_index = 9
        elsif pri_index > 9
          pri_index = 9
        end
      rescue
        pri_index = 9
      end
      record['level'] = ["emerg", "alert", "crit", "err", "warning", "notice", "info", "debug", "trace", "unknown"][pri_index]
      JOURNAL_TIME_FIELDS.each do |field|
        if record[field]
          record['time'] = Time.at(record[field].to_f / 1000000.0).utc.to_datetime.rfc3339(6)
          break
        end
      end
      if fmtr_type == :sys_journal
        record['message'] = record['MESSAGE']
        if record['_HOSTNAME'].eql?('localhost') && @docker_hostname
          record['hostname'] = @docker_hostname
        else
          record['hostname'] = record['_HOSTNAME']
        end
      elsif fmtr_type == :k8s_journal
        record['message'] = record['message'] || record['MESSAGE'] || record['log']
        if record['kubernetes'] && record['kubernetes']['host']
          record['hostname'] = record['kubernetes']['host']
        elsif @docker_hostname
          record['hostname'] = @docker_hostname
        else
          record['hostname'] = record['_HOSTNAME']
        end
      end
    end

    def process_sys_var_log_fields(tag, time, record, fmtr_type = nil)
      record['systemd'] = {"t" => {"PID" => record['pid']}, "u" => {"SYSLOG_IDENTIFIER" => record['ident']}}
      rectime = record['time'] || time
      # handle the case where the time reported in /var/log/messages is for a previous year
      if Time.at(rectime) > Time.now
        record['time'] = Time.new((rectime.year - 1), rectime.month, rectime.day, rectime.hour, rectime.min, rectime.sec, rectime.utc_offset).utc.to_datetime.rfc3339(6)
      else
        record['time'] = rectime.utc.to_datetime.rfc3339(6)
      end
      if record['host'].eql?('localhost') && @docker_hostname
        record['hostname'] = @docker_hostname
      else
        record['hostname'] = record['host']
      end
    end

    def process_k8s_json_file_fields(tag, time, record, fmtr_type = nil)
      record['message'] = record['message'] || record['log']
      record['level'] = (record['stream'] == 'stdout') ? 'info' : 'err'
      if record['kubernetes'] && record['kubernetes']['host']
        record['hostname'] = record['kubernetes']['host']
      elsif @docker_hostname
        record['hostname'] = @docker_hostname
      end
      record['time'] = record['time'].utc.to_datetime.rfc3339(6)
    end

    def check_for_match_and_format(tag, time, record)
      return unless @formatters
      return if @formatter_cache_nomatch[tag]
      fmtr = @formatter_cache[tag]
      unless fmtr
        idx = @formatters.index{|fmtr| fmtr.matcher.match(tag)}
        if idx
          fmtr = @formatters[idx]
          @formatter_cache[tag] = fmtr
        else
          @formatter_cache_nomatch[tag] = true
          return
        end
      end
      fmtr.fmtr_func.call(tag, time, record, fmtr.fmtr_type)

      if record['time'].nil?
        record['time'] = Time.at(time).utc.to_datetime.rfc3339(6)
      end

      if fmtr.fmtr_remove_keys
        fmtr.fmtr_remove_keys.each{|k| record.delete(k)}
      end
    end

    def add_pipeline_metadata (tag, time, record)
      (record['pipeline_metadata'] ||= {})[@pipeline_type.to_s] = {
        "ipaddr4"     => @ipaddr4,
        "ipaddr6"     => @ipaddr6,
        "inputname"   => "fluent-plugin-systemd",
        "name"        => "fluentd",
        "received_at" => Time.at(time).utc.to_datetime.rfc3339(6),
        "version"     => @pipeline_version
      }
    end

    def filter(tag, time, record)
      if ENV['CDM_DEBUG']
        unless tag == ENV['CDM_DEBUG_IGNORE_TAG']
          log.error("input #{time} #{tag} #{record}")
        end
      end

      check_for_match_and_format(tag, time, record)
      add_pipeline_metadata(tag, time, record)
      if @use_undefined
        # undefined contains all of the fields not in keep_fields
        undefined = record.reject{|k,v| @keep_fields.key?(k)}
        # only set the undefined field if there are undefined fields
        unless undefined.empty?
          record[@undefined_name] = undefined
          # remove the undefined fields from the record top level
          record.delete_if{|k,v| undefined.key?(k)}
        end
      end
      # remove the field from record if it is not in the list of fields to keep and
      # it is empty
      record.delete_if{|k,v| !@keep_empty_fields_hash.key?(k) && (v.nil? || isempty(delempty(v)) || isempty(v))}
      # probably shouldn't remove everything . . .
      log.warn("Empty record! tag [#{tag}] time [#{time}]") if record.empty?
      # rename the time field
      if (@rename_time || @rename_time_if_missing) && record.key?(@src_time_name)
        val = record.delete(@src_time_name)
        unless @rename_time_if_missing && record.key?(@dest_time_name)
          record[@dest_time_name] = val
        end
      end

      if @elasticsearch_index_field && !@elasticsearch_index_names.empty?
        found = false
        @elasticsearch_index_names.each do |ein|
          if ein.matcher.match(tag)
            record[@elasticsearch_index_field] = ein.func.call(record)
            found = true
            break
          end
        end
        unless found
          log.warn("no match for tag #{tag}")
        end
      end
      if ENV['CDM_DEBUG']
        unless tag == ENV['CDM_DEBUG_IGNORE_TAG']
          log.error("output #{time} #{tag} #{record}")
        end
      end
      record
    end
  end
end
