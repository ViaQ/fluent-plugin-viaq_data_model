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

  end
end
