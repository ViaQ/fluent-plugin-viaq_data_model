# fluent-plugin-viaq_data_model - a ViaQ data model filter plugin for [Fluentd](http://fluentd.org)
[![Travis CI](https://secure.travis-ci.org/ViaQ/fluent-plugin-viaq_data_model.png)](http://travis-ci.org/#!/ViaQ/fluent-plugin-viaq_data_model)

## Introduction

This plugin formats Fluentd records in the proper [ViaQ data
model](https://github.com/ViaQ/elasticsearch-templates).  It does the
following:

* Removes empty fields
  * fields with a value of `nil`
  * string fields with a value of `''` or the empty string
  * hash valued fields with a value of `{}`
  * hash valued fields which contain only empty fields as described above
  * FixNum, Boolean and other field values are not removed - type must respond
    to `:empty?` to be considered empty

* Moves "undefined" values to a top level field called `undefined`

The ViaQ data model wants all top level fields defined and described.  These
can conflict with the fields defined by ViaQ.  You can "move" these fields to
be under a hash valued top level field called `undefined` so as not to conflict
with the "well known" ViaQ top level fields.  You can optionally keep some
fields as top level fields while moving others to the `undefined` container.

* Rename a time field to `@timestamp`

You cannot set the `@timestamp` field in a Fluentd `record_transformer` filter.
The plugin allows you to use some other field e.g. `time` and have that "moved"
to a top level field called `@timestamp`.

* Converts systemd and json-file logs to ViaQ data model format

Doing this conversion in a `record_transformer` with embedded ruby code is very
resource intensive.  The ViaQ plugin can convert common input formats such as
Kubernetes `json-file`, `/var/log/messages`, and systemd `journald` into their
corresponding ViaQ `_default_`, `systemd`, `kubernetes`, and
`pipeline_metadata` namespaced fields.  The `pipeline_metadata` will be added
to all records, regardless of tag.  Use the `pipeline_type` parameter to
specify which part of the pipeline this is, `collector` or `normalizer`.
The ViaQ data model conversion will only be applied to matching `tag`s
specified in a `formatter` section.

* Creates Elasticsearch index names or prefixes

You can create either a full Elasticsearch index name for the record (to be
used with the `fluent-plugin-elasticsearch` `target_index_key` parameter), or
create an index name prefix (missing the date/timestamp part of the index
name - to be used with `logstash_prefix_key`).  In order to use this, create an
`elasticsearch_index_name` section, and specify the `tag` to match, and the
`name_type` type of index name to create.  By default, a prefix name will be
stored in the `viaq_index_prefix` field in the record, and a full name will be
stored in the `viaq_index_name` field.  Configure
`elasticsearch_index_name_field` or `elasticsearch_index_prefix_field` to use a
different field name.

## Configuration

NOTE: All fields are Optional - no required fields.

See `filter-viaq_data_model.conf` for an example filter configuration.

* `default_keep_fields` - comma delimited string - default: `''`
  * This is the default list of fields to keep as top level fields in the record
  * `default_keep_fields message,@timestamp,ident` - do not move these fields into the `undefined` field
* `extra_keep_fields` - comma delimited string - default: `''`
  * This is an extra list of fields to keep in addition to
  `default_keep_fields` - mostly useful as a way to hard code the
  `default_keep_fields` list for configuration management purposes, but at the
  same time allow customization in certain cases
  * `extra_keep_fields myspecialfield1,myspecialfield2`
* `keep_empty_fields` - comma delimited string - default `''`
  * Always keep these top level fields, even if they are empty
  * `keep_empty_fields message` - keep the `message` field, even if empty
* `use_undefined` - boolean - default `false`
  * If `true`, move fields not specified in `default_keep_fields` and
  `extra_keep_fields` to the `undefined` top level field.  If you use
  `use_undefined` you should specify the fields you want to keep out of
  `undefined` by using `default_keep_fields` and/or `extra_keep_fields`
* `undefined_name` - string - default `"undefined"`
  * Name of undefined top level field to use if `use_undefined true` is set
  * `undefined_name myfields` - keep undefined fields under field `myfields`
* `rename_time` - boolean - default `true`
  * Rename the time field e.g. when you need to set `@timestamp` in the record
  * NOTE: This will overwrite the `dest_time_name` if already set
* `rename_time_if_missing` - boolean - default `false`
  * Rename the time field only if it is not present.  For example, if some
  records already have the `@timestamp` field and you do not want to overwrite
  them, use `rename_time_if_missing true`
* `src_time_name` - string - default `time`
  * Use this field to get the value of the time field in the resulting record.
    This field will be removed from the record.
  * NOTE: This field must be present in the `default_keep_fields` or
  `extra_keep_fields` if `use_undefined true`
* `dest_time_name` - string - default `@timestamp`
  * This is the name of the top level field to hold the time value.  The value
  is taken from the value of the `src_time_name` field.
* `formatter` - a formatter for a well known common data model source
  * `enabled` - default `true` - is this formatter enabled?  **NOTE** if the
    formatter is disabled, it will still match, it just won't do anything, and
    it will skip the other formatters.
  * `type` - one of the well known sources
    * `sys_journal` - a record read from the systemd journal
    * `k8s_journal` - a Kubernetes container record read from the systemd
      journal - should have `CONTAINER_NAME`, `CONTAINER_ID_FULL`
    * `sys_var_log` - a record read from `/var/log/messages`
    * `k8s_json_file` - a record read from a `/var/log/containers/*.log` JSON
      formatted container log file
    * `tag` - the Fluentd tag pattern to match for these records
    * `remove_keys` - comma delimited list of keys to remove from the record
* `pipeline_type` - which part of the pipeline is this? `collector` or
  `normalizer` - the default is `collector`
* `elasticsearch_index_name` - how to construct Elasticsearch index names or
  prefixes for given tags
  * `enabled` - default `true` - is this item enabled?  **NOTE** if the
    item is disabled, it will still match, it just won't do anything, and
    it will skip the other index name items.
  * `tag` - the Fluentd tag pattern to match for these records
  * `name_type` - the well known type of index name or prefix to create -
    `operations_full, project_full, operations_prefix, project_prefix` - The
    `operations_*` types will create a name like `.operations`, and the
    `project_*` types will create a name like
    `project.record['kubernetes']['namespace_name'].record['kubernetes']['namespace_id']`.
    When using the `full` types, a delimiter `.` followed by the date in
    `YYYY.MM.DD` format will be added to the string to make a full index name.
    When using the `prefix` types, it is assumed that the
    `fluent-plugin-elaticsearch` is used with the `logstash_prefix_key` to
    create the full index name.
* `elasticsearch_index_name_field` - name of the field in the record which stores
  the index name - you should remove this field in the elasticsearch output
  plugin using the `remove_keys` config parameter - default is `viaq_idnex_name`
* `elasticsearch_index_prefix_field` - name of the field in the record which stores
  the index prefix - you should remove this field in the elasticsearch output
  plugin using the `remove_keys` config parameter - default is `viaq_idnex_prefix`

**NOTE** The `formatter` blocks are matched in the given order in the file.
  This means, don't use `tag "**"` as the first formatter or none of your
  others will be matched or evaulated.

**NOTE** The `elasticsearch_index_name` processing is done *last*, *after* the
  formatting, removal of empty fields, `@timestamp` creation, etc., so use
  e.g. `record['systemd']['t']['GID']` instead of `record['_GID']`

**NOTE** The `elasticsearch_index_name` blocks are matched in the given order
  in the file.  This means, don't use `tag "**"` as the first formatter or none
  of your others will be matched or evaulated.

## Example

If the input record looks like this:

    {
      "a": "b",
      "c": "d",
      "e": '',
      "f": {
        "g": '',
        "h": {}
      },
      "i": {
        "j": 0,
        "k": False,
        "l": ''
      },
      "time": "2017-02-13 15:30:10.259106596-07:00"
    }

The resulting record, using the defaults, would look like this:

    {
      "a": "b",
      "c": "d",
      "i": {
        "j": 0,
        "k": False,
      },
      "@timestamp": "2017-02-13 15:30:10.259106596-07:00"
    }

## Formatter example

Given a record like the following with a tag of `journal.system`

    __REALTIME_TIMESTAMP=1502228121310282
    __MONOTONIC_TIMESTAMP=722903835100
    _BOOT_ID=d85e8a9d524c4a419bcfb6598db78524
    _TRANSPORT=syslog
    PRIORITY=6
    SYSLOG_FACILITY=3
    SYSLOG_IDENTIFIER=dnsmasq-dhcp
    SYSLOG_PID=2289
    _PID=2289
    _UID=99
    _GID=40
    _COMM=dnsmasq
    _EXE=/usr/sbin/dnsmasq
    _CMDLINE=/sbin/dnsmasq --conf-file=/var/lib/libvirt/dnsmasq/default.conf --leasefile-ro --dhcp-script=/usr/libexec/libvirt_leaseshelper
    _CAP_EFFECTIVE=3400
    _SYSTEMD_CGROUP=/system.slice/libvirtd.service
    MESSASGE=my message

Using a configuration like this:

    <formatter>
      tag "journal.system**"
      type sys_journal
      remove_keys log,stream,MESSAGE,_SOURCE_REALTIME_TIMESTAMP,__REALTIME_TIMESTAMP,CONTAINER_ID,CONTAINER_ID_FULL,CONTAINER_NAME,PRIORITY,_BOOT_ID,_CAP_EFFECTIVE,_CMDLINE,_COMM,_EXE,_GID,_HOSTNAME,_MACHINE_ID,_PID,_SELINUX_CONTEXT,_SYSTEMD_CGROUP,_SYSTEMD_SLICE,_SYSTEMD_UNIT,_TRANSPORT,_UID,_AUDIT_LOGINUID,_AUDIT_SESSION,_SYSTEMD_OWNER_UID,_SYSTEMD_SESSION,_SYSTEMD_USER_UNIT,CODE_FILE,CODE_FUNCTION,CODE_LINE,ERRNO,MESSAGE_ID,RESULT,UNIT,_KERNEL_DEVICE,_KERNEL_SUBSYSTEM,_UDEV_SYSNAME,_UDEV_DEVNODE,_UDEV_DEVLINK,SYSLOG_FACILITY,SYSLOG_IDENTIFIER,SYSLOG_PID
    </formatter>

The resulting record will look like this:

    {
    "systemd": {
      "t": {
        "BOOT_ID":"d85e8a9d524c4a419bcfb6598db78524",
        "GID":40,
        ...
      },
      "u": {
        "SYSLOG_FACILITY":3,
        "SYSLOG_IDENTIFIER":"dnsmasq-dhcp",
        ...
      },
    "message":"my message",
    ...
    }

## Elasticsearch index name example

Given a configuration like this:

    <elasticsearch_index_name>
      tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
      name_type operations_full
    </elasticsearch_index_name>
    <elasticsearch_index_name>
      tag "**"
      name_type project_full
    </elasticsearch_index_name>

A record with tag `journal.system` like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00"
    }

will end up looking like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
      "viaq_index_name":".operations.2017.07.07"
    }

A record with tag `kubernetes.journal.container` like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
      "kubernetes":{"namespace_name":"myproject","namespace_id":"000000"}
    }

will end up looking like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
      "kubernetes":{"namespace_name":"myproject","namespace_id":"000000"}
      "viaq_index_name":"project.myproject.000000.2017.07.07"
    }


### Note about using enabled false

Given a configuration like this:

    <elasticsearch_index_name>
      enabled false
      tag "journal.system** system.var.log** **_default_** **_openshift_** **_openshift-infra_** mux.ops"
      name_type operations_full
    </elasticsearch_index_name>
    <elasticsearch_index_name>
      tag "**"
      name_type project_full
    </elasticsearch_index_name>

A record with tag `journal.system` like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00"
    }

will end up looking like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
    }

That is, the tag will match the first `elasticsearch_index_name`, but since it
is disabled, no index name will be created, and it will _not_ fall through to
the `**` match below.  Using `enabled false` in this case allows you to not
generate index names for operations indices, but still continue to generate
index names for project indices.

A record with tag `kubernetes.journal.container` like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
      "kubernetes":{"namespace_name":"myproject","namespace_id":"000000"}
    }

will end up looking like this:

    {
      "@timestamp":"2017-07-27T17:27:46.216527+00:00",
      "kubernetes":{"namespace_name":"myproject","namespace_id":"000000"}
      "viaq_index_name":"project.myproject.000000.2017.07.07"
    }

## Installation

    gem install fluent-plugin-viaq_data_model

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Test it (`GEM_HOME=vendor bundle install; GEM_HOME=vendor bundle exec rake test`)
5. Push to the branch (`git push origin my-new-feature`)
6. Create new Pull Request
