#
# Fluentd Viaq Data Model Filter Plugin - Ensure records coming from Fluentd
# use the correct Viaq data model formatting and fields.
#
# Copyright 2016 Red Hat, Inc.
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
#require_relative '../helper'
require 'fluent/test'

require 'fluent/plugin/filter_viaq_data_model'

class ViaqDataModelFilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Fluent::Test.setup
    @time = Fluent::Engine.now
    log = Fluent::Engine.log
  end

  def create_driver(conf = '')
    Test::FilterTestDriver.new(ViaqDataModelFilter, 'this.is.a.tag').configure(conf, true)
  end

  sub_test_case 'configure' do
    test 'check default' do
      d = create_driver
      assert_equal([], d.instance.default_keep_fields)
      assert_equal([], d.instance.extra_keep_fields)
      assert_equal(['message'], d.instance.keep_empty_fields)
      assert_equal(false, d.instance.use_undefined)
      assert_equal('undefined', d.instance.undefined_name)
      assert_equal(true, d.instance.rename_time)
      assert_equal('time', d.instance.src_time_name)
      assert_equal('@timestamp', d.instance.dest_time_name)
    end
    test 'check various settings' do
      d = create_driver('
        default_keep_fields a,b,c
        extra_keep_fields d,e,f
        keep_empty_fields g,h,i
        use_undefined true
        undefined_name j
        rename_time false
        src_time_name k
        dest_time_name l
      ')
      assert_equal(['a','b','c'], d.instance.default_keep_fields)
      assert_equal(['d','e','f'], d.instance.extra_keep_fields)
      assert_equal(['g','h','i'], d.instance.keep_empty_fields)
      assert_equal(true, d.instance.use_undefined)
      assert_equal('j', d.instance.undefined_name)
      assert_equal(false, d.instance.rename_time)
      assert_equal('k', d.instance.src_time_name)
      assert_equal('l', d.instance.dest_time_name)
    end
    test 'error if undefined_name in default_keep_fields' do
      assert_raise(Fluent::ConfigError) {
        d = create_driver('
          default_keep_fields a
          use_undefined true
          undefined_name a
        ')
      }
    end
    test 'error if undefined_name in extra_keep_fields' do
      assert_raise(Fluent::ConfigError) {
        d = create_driver('
          extra_keep_fields a
          use_undefined true
          undefined_name a
        ')
      }
    end
    test 'error if src_time_field not in default_keep_fields' do
      assert_raise(Fluent::ConfigError) {
        d = create_driver('
          default_keep_fields a
          use_undefined true
          rename_time true
          src_time_name b
        ')
      }
    end
    test 'error if src_time_field not in extra_keep_fields' do
      assert_raise(Fluent::ConfigError) {
        d = create_driver('
          extra_keep_fields a
          use_undefined true
          rename_time true
          src_time_name b
        ')
      }
    end
  end

  sub_test_case 'filtering' do
    def emit_with_tag(tag, msg={}, conf='')
      d = create_driver(conf)
      d.run {
        d.emit_with_tag(tag, msg, @time)
      }.filtered.instance_variable_get(:@record_array)[0]
    end
    test 'see if undefined fields are kept at top level' do
      rec = emit_with_tag('tag', {'a'=>'b'})
      assert_equal('b', rec['a'])
    end
    test 'see if undefined fields are put in undefined field except for kept fields' do
      rec = emit_with_tag('tag', {'a'=>'b','c'=>'d','e'=>'f'}, '
        use_undefined true
        default_keep_fields c
        extra_keep_fields e
        rename_time false
      ')
      assert_equal('b', rec['undefined']['a'])
      assert_equal('d', rec['c'])
      assert_equal('f', rec['e'])
    end
    test 'see if undefined fields are put in custom field except for kept fields' do
      rec = emit_with_tag('tag', {'a'=>'b','c'=>'d','e'=>'f'}, '
        use_undefined true
        undefined_name custom
        default_keep_fields c
        extra_keep_fields e
        rename_time false
      ')
      assert_equal('b', rec['custom']['a'])
      assert_equal('d', rec['c'])
      assert_equal('f', rec['e'])
    end
    test 'see if specified empty fields are kept at top level' do
      rec = emit_with_tag('tag', {'a'=>'b','c'=>'','d'=>{}}, '
        keep_empty_fields c,d
      ')
      assert_equal('b', rec['a'])
      assert_equal('', rec['c'])
      assert_equal({}, rec['d'])
    end
    test 'see if time field is renamed' do
      rec = emit_with_tag('tag', {'a'=>'b'}, '
        rename_time true
        src_time_name a
        dest_time_name c
      ')
      assert_equal('b', rec['c'])
      assert_nil(rec['a'])
    end
    test 'see if time field is renamed when checking if missing' do
      rec = emit_with_tag('tag', {'a'=>'b'}, '
        rename_time_if_missing true
        src_time_name a
        dest_time_name c
      ')
      assert_equal('b', rec['c'])
      assert_nil(rec['a'])
    end
    test 'see if time field is renamed when already present' do
      rec = emit_with_tag('tag', {'a'=>'b','c'=>'d'}, '
        rename_time true
        src_time_name a
        dest_time_name c
      ')
      assert_equal('b', rec['c'])
      assert_nil(rec['a'])
    end
    test 'see if time field is preserved when already present' do
      rec = emit_with_tag('tag', {'a'=>'b','c'=>'d'}, '
        rename_time_if_missing true
        src_time_name a
        dest_time_name c
      ')
      assert_equal('d', rec['c'])
      assert_nil(rec['a'])
    end
    test 'see if deeply nested empty fields are removed or preserved' do
      msg = {'a'=>{'b'=>{'c'=>{'d'=>{'e'=>'','f'=>{},'g'=>''}}}},'h'=>{'i'=>{'j'=>'','k'=>'l','m'=>99,'n'=>true}}}
      rec = emit_with_tag('tag', msg)
      assert_nil(rec['a'])
      assert_equal('l', rec['h']['i']['k'])
      assert_equal(99, rec['h']['i']['m'])
      assert_true(rec['h']['i']['n'])
    end
    test 'see if fields with a value of numeric 0 are removed or preserved' do
      msg = {'a'=>{'b'=>{'c'=>{'d'=>{'e'=>'','f'=>{},'g'=>0}}}},'h'=>{'i'=>{'j'=>'','k'=>'l','m'=>0,'n'=>true}}}
      rec = emit_with_tag('tag', msg)
      assert_nil(rec['a']['b']['c']['d']['e'])
      assert_nil(rec['a']['b']['c']['d']['f'])
      assert_equal(0, rec['a']['b']['c']['d']['g'])
      assert_equal('l', rec['h']['i']['k'])
      assert_equal(0, rec['h']['i']['m'])
      assert_true(rec['h']['i']['n'])
    end
    test 'see if fields with array values of numeric values are preserved' do
      msg = {'a'=>{'b'=>{'c'=>{'d'=>{'e'=>'','f'=>{},'g'=>[99.999]}}}},'h'=>{'i'=>{'j'=>'','k'=>'l','m'=>[88],'n'=>true}}}
      rec = emit_with_tag('tag', msg)
      assert_equal([99.999], rec['a']['b']['c']['d']['g'])
      assert_nil(rec['a']['b']['c']['d']['e'])
      assert_nil(rec['a']['b']['c']['d']['f'])
      assert_equal('l', rec['h']['i']['k'])
      assert_equal([88], rec['h']['i']['m'])
      assert_true(rec['h']['i']['n'])
    end
  end

  sub_test_case 'journal' do
    def emit_with_tag(tag, msg={}, conf='')
      d = create_driver(conf)
      d.run {
        d.emit_with_tag(tag, msg, @time)
      }.filtered.instance_variable_get(:@record_array)[0]
    end

    def setup
      # elasticsearch index constructors use $log, so have to fake it
      @orig_log = $log
      $log = Fluent::Test::TestLogger.new
    end

    def teardown
      $log = @orig_log
    end

    def normal_input
      {
        "_AUDIT_LOGINUID"            => "AUDIT_LOGINUID",
        "_AUDIT_SESSION"             => "AUDIT_SESSION",
        "_BOOT_ID"                   => "BOOT_ID",
        "_CAP_EFFECTIVE"             => "CAP_EFFECTIVE",
        "_CMDLINE"                   => "CMDLINE",
        "_COMM"                      => "COMM",
        "_EXE"                       => "EXE",
        "_GID"                       => "GID",
        "_MACHINE_ID"                => "MACHINE_ID",
        "_PID"                       => "PID",
        "_SELINUX_CONTEXT"           => "SELINUX_CONTEXT",
        "_SYSTEMD_CGROUP"            => "SYSTEMD_CGROUP",
        "_SYSTEMD_OWNER_UID"         => "SYSTEMD_OWNER_UID",
        "_SYSTEMD_SESSION"           => "SYSTEMD_SESSION",
        "_SYSTEMD_SLICE"             => "SYSTEMD_SLICE",
        "_SYSTEMD_UNIT"              => "SYSTEMD_UNIT",
        "_SYSTEMD_USER_UNIT"         => "SYSTEMD_USER_UNIT",
        "_TRANSPORT"                 => "TRANSPORT",
        "_UID"                       => "UID",
        "CODE_FILE"                  => "CODE_FILE",
        "CODE_FUNCTION"              => "CODE_FUNCTION",
        "CODE_LINE"                  => "CODE_LINE",
        "ERRNO"                      => "ERRNO",
        "MESSAGE_ID"                 => "MESSAGE_ID",
        "RESULT"                     => "RESULT",
        "UNIT"                       => "UNIT",
        "SYSLOG_FACILITY"            => "SYSLOG_FACILITY",
        "SYSLOG_IDENTIFIER"          => "SYSLOG_IDENTIFIER",
        "SYSLOG_PID"                 => "SYSLOG_PID",
        "_KERNEL_DEVICE"             => "KERNEL_DEVICE",
        "_KERNEL_SUBSYSTEM"          => "KERNEL_SUBSYSTEM",
        "_UDEV_SYSNAME"              => "UDEV_SYSNAME",
        "_UDEV_DEVNODE"              => "UDEV_DEVNODE",
        "_UDEV_DEVLINK"              => "UDEV_DEVLINK",
        "_SOURCE_REALTIME_TIMESTAMP" => "1501176466216527",
        "__REALTIME_TIMESTAMP"       => "1501176466216527",
        "MESSAGE"                    => "hello world",
        "PRIORITY"                   => "6",
        "_HOSTNAME"                  => "myhost"
      }
    end
    def normal_output_t
      {
        "AUDIT_LOGINUID"    =>"AUDIT_LOGINUID",
        "AUDIT_SESSION"     =>"AUDIT_SESSION",
        "BOOT_ID"           =>"BOOT_ID",
        "CAP_EFFECTIVE"     =>"CAP_EFFECTIVE",
        "CMDLINE"           =>"CMDLINE",
        "COMM"              =>"COMM",
        "EXE"               =>"EXE",
        "GID"               =>"GID",
        "MACHINE_ID"        =>"MACHINE_ID",
        "PID"               =>"PID",
        "SELINUX_CONTEXT"   =>"SELINUX_CONTEXT",
        "SYSTEMD_CGROUP"    =>"SYSTEMD_CGROUP",
        "SYSTEMD_OWNER_UID" =>"SYSTEMD_OWNER_UID",
        "SYSTEMD_SESSION"   =>"SYSTEMD_SESSION",
        "SYSTEMD_SLICE"     =>"SYSTEMD_SLICE",
        "SYSTEMD_UNIT"      =>"SYSTEMD_UNIT",
        "SYSTEMD_USER_UNIT" =>"SYSTEMD_USER_UNIT",
        "TRANSPORT"         =>"TRANSPORT",
        "UID"               =>"UID"
      }
    end
    def normal_output_u
      {
        "CODE_FILE"         =>"CODE_FILE",
        "CODE_FUNCTION"     =>"CODE_FUNCTION",
        "CODE_LINE"         =>"CODE_LINE",
        "ERRNO"             =>"ERRNO",
        "MESSAGE_ID"        =>"MESSAGE_ID",
        "RESULT"            =>"RESULT",
        "UNIT"              =>"UNIT",
        "SYSLOG_FACILITY"   =>"SYSLOG_FACILITY",
        "SYSLOG_IDENTIFIER" =>"SYSLOG_IDENTIFIER",
        "SYSLOG_PID"        =>"SYSLOG_PID"
      }
    end
    def normal_output_k
      {
        "KERNEL_DEVICE"    =>"KERNEL_DEVICE",
        "KERNEL_SUBSYSTEM" =>"KERNEL_SUBSYSTEM",
        "UDEV_SYSNAME"     =>"UDEV_SYSNAME",
        "UDEV_DEVNODE"     =>"UDEV_DEVNODE",
        "UDEV_DEVLINK"     =>"UDEV_DEVLINK"
      }
    end
    test 'match records with journal_system_record_tag' do
      rec = emit_with_tag('journal.system', {'a'=>'b', 'MESSAGE'=>'here'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('here', rec['message'])
    end
    test 'do not match records without journal_system_record_tag' do
      rec = emit_with_tag('journal.systm', {'a'=>'b', 'MESSAGE'=>'here'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('here', rec['MESSAGE'])
    end
    test 'process a journal record, default settings' do
      ENV['IPADDR4'] = '127.0.0.1'
      ENV['IPADDR6'] = '::1'
      ENV['FLUENTD_VERSION'] = 'fversion'
      ENV['DATA_VERSION'] = 'dversion'
      rec = emit_with_tag('journal.system', normal_input, '
        journal_system_record_tag "journal.system**"
        pipeline_type normalizer
      ')
      assert_equal(rec['systemd']['t'], normal_output_t)
      assert_equal(rec['systemd']['u'], normal_output_u)
      assert_equal(rec['systemd']['k'], normal_output_k)
      assert_equal(rec['message'], 'hello world')
      assert_equal(rec['level'], 'info')
      assert_equal(rec['hostname'], 'myhost')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr4'], '127.0.0.1')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr6'], '::1')
      assert_equal(rec['pipeline_metadata']['normalizer']['inputname'], 'fluent-plugin-systemd')
      assert_equal(rec['pipeline_metadata']['normalizer']['name'], 'fluentd')
      assert_equal(rec['pipeline_metadata']['normalizer']['version'], 'fversion dversion')
      assert_equal(rec['pipeline_metadata']['normalizer']['received_at'], Time.at(@time).utc.to_datetime.rfc3339(6))
      dellist = 'log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID'.split(',')
      dellist.each{|field| assert_nil(rec[field])}
    end
    test 'process a journal record, override remove_keys' do
      ENV['IPADDR4'] = '127.0.0.1'
      ENV['IPADDR6'] = '::1'
      ENV['FLUENTD_VERSION'] = 'fversion'
      ENV['DATA_VERSION'] = 'dversion'
      rec = emit_with_tag('journal.system', normal_input, '
        journal_system_record_tag "journal.system**"
        pipeline_type normalizer
        remove_keys CONTAINER_NAME,PRIORITY
      ')
      assert_equal(rec['systemd']['t'], normal_output_t)
      assert_equal(rec['systemd']['u'], normal_output_u)
      assert_equal(rec['systemd']['k'], normal_output_k)
      assert_equal(rec['message'], 'hello world')
      assert_equal(rec['level'], 'info')
      assert_equal(rec['hostname'], 'myhost')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr4'], '127.0.0.1')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr6'], '::1')
      assert_equal(rec['pipeline_metadata']['normalizer']['inputname'], 'fluent-plugin-systemd')
      assert_equal(rec['pipeline_metadata']['normalizer']['name'], 'fluentd')
      assert_equal(rec['pipeline_metadata']['normalizer']['version'], 'fversion dversion')
      assert_equal(rec['pipeline_metadata']['normalizer']['received_at'], Time.at(@time).utc.to_datetime.rfc3339(6))
      keeplist = 'log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID'.split(',')
      keeplist.each{|field| normal_input[field] && assert_not_nil(rec[field])}
      dellist = 'CONTAINER_NAME,PRIORITY'.split(',')
      dellist.each{|field| assert_nil(rec[field])}
    end
    test 'try a PRIORITY value that is too large' do
      rec = emit_with_tag('journal.system', {'a'=>'b', 'PRIORITY'=>'10'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('unknown', rec['level'])
    end
    test 'try a PRIORITY value that is too small' do
      rec = emit_with_tag('journal.system', {'a'=>'b', 'PRIORITY'=>'-1'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('unknown', rec['level'])
    end
    test 'try a PRIORITY value that is not a number' do
      rec = emit_with_tag('journal.system', {'a'=>'b', 'PRIORITY'=>'NaN'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('unknown', rec['level'])
    end
    test 'try a PRIORITY value that is a floating point number' do
      rec = emit_with_tag('journal.system', {'a'=>'b', 'PRIORITY'=>'1.0'}, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal('b', rec['a'])
      assert_equal('unknown', rec['level'])
    end
    test 'test with fallback to __REALTIME_TIMESTAMP' do
      input = normal_input.reject{|k,v| k == '_SOURCE_REALTIME_TIMESTAMP'}
      rec = emit_with_tag('journal.system', input, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
    end
    test 'test using internal time if no timestamp given' do
      input = normal_input.reject do |k,v|
        k == '_SOURCE_REALTIME_TIMESTAMP' || k == '__REALTIME_TIMESTAMP'
      end
      rec = emit_with_tag('journal.system', input, '
        journal_system_record_tag "journal.system**"
      ')
      assert_equal(rec['@timestamp'], Time.at(@time).utc.to_datetime.rfc3339(6))
    end
    test 'process a kubernetes journal record, default settings' do
      ENV['IPADDR4'] = '127.0.0.1'
      ENV['IPADDR6'] = '::1'
      ENV['FLUENTD_VERSION'] = 'fversion'
      ENV['DATA_VERSION'] = 'dversion'
      rec = emit_with_tag('kubernetes.journal.container', normal_input, '
        journal_k8s_record_tag "kubernetes.journal.container**"
        pipeline_type normalizer
      ')
      assert_equal(rec['systemd']['t'], normal_output_t)
      assert_equal(rec['systemd']['u'], normal_output_u)
      assert_equal(rec['systemd']['k'], normal_output_k)
      assert_equal(rec['message'], 'hello world')
      assert_equal(rec['level'], 'info')
      assert_equal(rec['hostname'], 'myhost')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr4'], '127.0.0.1')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr6'], '::1')
      assert_equal(rec['pipeline_metadata']['normalizer']['inputname'], 'fluent-plugin-systemd')
      assert_equal(rec['pipeline_metadata']['normalizer']['name'], 'fluentd')
      assert_equal(rec['pipeline_metadata']['normalizer']['version'], 'fversion dversion')
      assert_equal(rec['pipeline_metadata']['normalizer']['received_at'], Time.at(@time).utc.to_datetime.rfc3339(6))
      dellist = 'log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID'.split(',')
      dellist.each{|field| assert_nil(rec[field])}
    end
    test 'process a kubernetes journal record, given kubernetes.host' do
      input = normal_input.merge({})
      input['kubernetes'] = {'host' => 'k8shost'}
      ENV['IPADDR4'] = '127.0.0.1'
      ENV['IPADDR6'] = '::1'
      ENV['FLUENTD_VERSION'] = 'fversion'
      ENV['DATA_VERSION'] = 'dversion'
      rec = emit_with_tag('kubernetes.journal.container', input, '
        journal_k8s_record_tag "kubernetes.journal.container**"
        pipeline_type normalizer
      ')
      assert_equal(rec['systemd']['t'], normal_output_t)
      assert_equal(rec['systemd']['u'], normal_output_u)
      assert_equal(rec['systemd']['k'], normal_output_k)
      assert_equal(rec['message'], 'hello world')
      assert_equal(rec['level'], 'info')
      assert_equal(rec['hostname'], 'k8shost')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr4'], '127.0.0.1')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr6'], '::1')
      assert_equal(rec['pipeline_metadata']['normalizer']['inputname'], 'fluent-plugin-systemd')
      assert_equal(rec['pipeline_metadata']['normalizer']['name'], 'fluentd')
      assert_equal(rec['pipeline_metadata']['normalizer']['version'], 'fversion dversion')
      assert_equal(rec['pipeline_metadata']['normalizer']['received_at'], Time.at(@time).utc.to_datetime.rfc3339(6))
      dellist = 'log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID'.split(',')
      dellist.each{|field| assert_nil(rec[field])}
    end
    test 'process a kubernetes journal record, preserve message field' do
      input = normal_input.merge({})
      input['message'] = 'my message'
      ENV['IPADDR4'] = '127.0.0.1'
      ENV['IPADDR6'] = '::1'
      ENV['FLUENTD_VERSION'] = 'fversion'
      ENV['DATA_VERSION'] = 'dversion'
      rec = emit_with_tag('kubernetes.journal.container', input, '
        journal_k8s_record_tag "kubernetes.journal.container**"
        pipeline_type normalizer
      ')
      assert_equal(rec['systemd']['t'], normal_output_t)
      assert_equal(rec['systemd']['u'], normal_output_u)
      assert_equal(rec['systemd']['k'], normal_output_k)
      assert_equal(rec['message'], 'my message')
      assert_equal(rec['level'], 'info')
      assert_equal(rec['hostname'], 'myhost')
      assert_equal(rec['@timestamp'], '2017-07-27T17:27:46.216527+00:00')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr4'], '127.0.0.1')
      assert_equal(rec['pipeline_metadata']['normalizer']['ipaddr6'], '::1')
      assert_equal(rec['pipeline_metadata']['normalizer']['inputname'], 'fluent-plugin-systemd')
      assert_equal(rec['pipeline_metadata']['normalizer']['name'], 'fluentd')
      assert_equal(rec['pipeline_metadata']['normalizer']['version'], 'fversion dversion')
      assert_equal(rec['pipeline_metadata']['normalizer']['received_at'], Time.at(@time).utc.to_datetime.rfc3339(6))
      dellist = 'log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID'.split(',')
      dellist.each{|field| assert_nil(rec[field])}
    end
    test 'expect error from elasticsearch_index_field but no elasticsearch_index_names' do
      assert_raise(Fluent::ConfigError) {
        rec = emit_with_tag('journal.system', normal_input, '
            elasticsearch_index_field viaq_index_name
        ')
      }
    end
    test 'expect error from elasticsearch_index_names but no elasticsearch_index_field' do
      assert_raise(Fluent::ConfigError) {
        rec = emit_with_tag('journal.system', normal_input, '
            <elasticsearch_index_name>
              tag "junk"
              code "\'word\'"
            </elasticsearch_index_name>
        ')
      }
    end
    test 'expect error from missing elasticsearch_index_names code' do
      assert_raise(Fluent::ConfigError) {
        rec = emit_with_tag('journal.system', normal_input, '
            <elasticsearch_index_name>
              tag "junk"
            </elasticsearch_index_name>
            elasticsearch_index_field junk
        ')
      }
    end
    test 'expect error from missing elasticsearch_index_names tag' do
      assert_raise(Fluent::ConfigError) {
        rec = emit_with_tag('journal.system', normal_input, '
            <elasticsearch_index_name>
              code "junk"
            </elasticsearch_index_name>
            elasticsearch_index_field junk
        ')
      }
    end
    test 'construct an operations index name' do
      rec = emit_with_tag('journal.system', normal_input, '
        journal_system_record_tag "journal.system**"
        journal_k8s_record_tag "kubernetes.journal.container**"
        <elasticsearch_index_name>
          tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
          code "begin \'.operations.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) rescue $log.error(\'record is missing time and @timestamp - record \' + record.to_s) end"
        </elasticsearch_index_name>
        <elasticsearch_index_name>
          tag "**"
          code "if record[\'kubernetes\'].nil?; $log.error(\'record is missing kubernetes field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_name\'].nil?; $log.error(\'record is missing kubernetes.namespace_name field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_id\'].nil?; $log.error(\'record is missing kubernetes.namespace_id field \' + record.to_s); elsif (record[\'@timestamp\'].nil? || record[\'time\'].nil?); $log.error(\'record is missing @timestamp and time fields \' + record.to_s); else \'project.\' + record[\'kubernetes\'][\'namespace_name\'] + \'.\' + record[\'kubernetes\'][\'namespace_id\'] + \'.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) end"
        </elasticsearch_index_name>
        elasticsearch_index_field viaq_index_name
      ')
      assert_equal(rec['viaq_index_name'], '.operations.2017.07.27')
    end
    test 'log error if missing kubernetes field' do
      # elasticsearch index constructors use $log, so have to fake it
      orig_log = $log
      $log = Fluent::Test::TestLogger.new
      rec = emit_with_tag('kubernetes.journal.container.something', normal_input, '
        journal_system_record_tag "journal.system**"
        journal_k8s_record_tag "kubernetes.journal.container**"
        <elasticsearch_index_name>
          tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
          code "begin \'.operations.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) rescue $log.error(\'record is missing time and @timestamp - record \' + record.to_s) end"
        </elasticsearch_index_name>
        <elasticsearch_index_name>
          tag "**"
          code "if record[\'kubernetes\'].nil?; $log.error(\'record is missing kubernetes field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_name\'].nil?; $log.error(\'record is missing kubernetes.namespace_name field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_id\'].nil?; $log.error(\'record is missing kubernetes.namespace_id field \' + record.to_s); elsif (record[\'@timestamp\'].nil? || record[\'time\'].nil?); $log.error(\'record is missing @timestamp and time fields \' + record.to_s); else \'project.\' + record[\'kubernetes\'][\'namespace_name\'] + \'.\' + record[\'kubernetes\'][\'namespace_id\'] + \'.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) end"
        </elasticsearch_index_name>
        elasticsearch_index_field viaq_index_name
      ')
      assert_match /record is missing kubernetes field/, $log.logs[0]
      $log = orig_log
    end
    test 'log error if missing kubernetes.namespace_name field' do
      # elasticsearch index constructors use $log, so have to fake it
      orig_log = $log
      $log = Fluent::Test::TestLogger.new
      input = normal_input.merge({})
      input['kubernetes'] = 'junk'
      rec = emit_with_tag('kubernetes.journal.container.something', input, '
        journal_system_record_tag "journal.system**"
        journal_k8s_record_tag "kubernetes.journal.container**"
        <elasticsearch_index_name>
          tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
          code "begin \'.operations.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) rescue $log.error(\'record is missing time and @timestamp - record \' + record.to_s) end"
        </elasticsearch_index_name>
        <elasticsearch_index_name>
          tag "**"
          code "if record[\'kubernetes\'].nil?; $log.error(\'record is missing kubernetes field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_name\'].nil?; $log.error(\'record is missing kubernetes.namespace_name field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_id\'].nil?; $log.error(\'record is missing kubernetes.namespace_id field \' + record.to_s); elsif (record[\'@timestamp\'].nil? || record[\'time\'].nil?); $log.error(\'record is missing @timestamp and time fields \' + record.to_s); else \'project.\' + record[\'kubernetes\'][\'namespace_name\'] + \'.\' + record[\'kubernetes\'][\'namespace_id\'] + \'.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) end"
        </elasticsearch_index_name>
        elasticsearch_index_field viaq_index_name
      ')
      assert_match /record is missing kubernetes.namespace_name field/, $log.logs[0]
      $log = orig_log
    end
    test 'log error if missing kubernetes.namespace_id field' do
      # elasticsearch index constructors use $log, so have to fake it
      orig_log = $log
      $log = Fluent::Test::TestLogger.new
      input = normal_input.merge({})
      input['kubernetes'] = {'namespace_name'=>'junk'}
      rec = emit_with_tag('kubernetes.journal.container.something', input, '
        journal_system_record_tag "journal.system**"
        journal_k8s_record_tag "kubernetes.journal.container**"
        <elasticsearch_index_name>
          tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
          code "begin \'.operations.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) rescue $log.error(\'record is missing time and @timestamp - record \' + record.to_s) end"
        </elasticsearch_index_name>
        <elasticsearch_index_name>
          tag "**"
          code "if record[\'kubernetes\'].nil?; $log.error(\'record is missing kubernetes field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_name\'].nil?; $log.error(\'record is missing kubernetes.namespace_name field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_id\'].nil?; $log.error(\'record is missing kubernetes.namespace_id field \' + record.to_s); elsif (record[\'@timestamp\'].nil? || record[\'time\'].nil?); $log.error(\'record is missing @timestamp and time fields \' + record.to_s); else \'project.\' + record[\'kubernetes\'][\'namespace_name\'] + \'.\' + record[\'kubernetes\'][\'namespace_id\'] + \'.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) end"
        </elasticsearch_index_name>
        elasticsearch_index_field viaq_index_name
      ')
      assert_match /record is missing kubernetes.namespace_id field/, $log.logs[0]
      $log = orig_log
    end
    test 'construct a kubernetes index name field' do
      # elasticsearch index constructors use $log, so have to fake it
      input = normal_input.merge({})
      input['kubernetes'] = {'namespace_name'=>'name', 'namespace_id'=>'uuid'}
      rec = emit_with_tag('kubernetes.journal.container.something', input, '
        journal_system_record_tag "journal.system**"
        journal_k8s_record_tag "kubernetes.journal.container**"
        <elasticsearch_index_name>
          tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
          code "begin \'.operations.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) rescue $log.error(\'record is missing time and @timestamp - record \' + record.to_s) end"
        </elasticsearch_index_name>
        <elasticsearch_index_name>
          tag "**"
          code "if record[\'kubernetes\'].nil?; $log.error(\'record is missing kubernetes field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_name\'].nil?; $log.error(\'record is missing kubernetes.namespace_name field \' + record.to_s); elsif record[\'kubernetes\'][\'namespace_id\'].nil?; $log.error(\'record is missing kubernetes.namespace_id field \' + record.to_s); elsif (record[\'@timestamp\'].nil? && record[\'time\'].nil?); $log.error(\'record is missing @timestamp and time fields \' + record.to_s); else \'project.\' + record[\'kubernetes\'][\'namespace_name\'] + \'.\' + record[\'kubernetes\'][\'namespace_id\'] + \'.\' + (record[\'@timestamp\'].nil? ? Time.at(time).getutc.strftime(\'%Y.%m.%d\') : Time.parse(record[\'@timestamp\']).getutc.strftime(\'%Y.%m.%d\')) end"
        </elasticsearch_index_name>
        elasticsearch_index_field viaq_index_name
      ')
      assert_equal(rec['viaq_index_name'], 'project.name.uuid.2017.07.27')
    end
  end
end
