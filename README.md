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


## Installation

    gem install fluent-plugin-viaq_data_model

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Test it (`GEM_HOME=vendor bundle install; GEM_HOME=vendor bundle exec rake test`)
5. Push to the branch (`git push origin my-new-feature`)
6. Create new Pull Request
