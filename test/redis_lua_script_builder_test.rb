require 'minitest/autorun'
require './lib/redis_lua_script_builder'
require './lib/redis_writer'

def array_sections_equal(set, *sections)
  base_index = 0
  sections.each do |section|
    return false if set[base_index..(base_index + section.size - 1)].sort != section.sort
    base_index += section.size
  end
  base_index == set.size
end

describe RedisLuaScriptBuilder do
  before(:each) do
    @rlsb = RedisLuaScriptBuilder.new("Testing")
    @rlsb.debug_mode = false

    @rw   = RedisWriter.new
    @rw.force_db_reset
  end

  describe "#invalid_query?" do
    it "must be invalid if sinter_keys is empty" do
      assert @rlsb.invalid_query?
      assert @rlsb.sinter_keys.empty?
      @rlsb.add_table_existence_requirement_query(:amenity_park)
      assert !@rlsb.invalid_query?
      assert !@rlsb.sinter_keys.empty?
    end
  end

  describe "#valid_query?" do
    it "must be valid if sinter_keys is not empty" do
      assert !@rlsb.valid_query?
      assert @rlsb.sinter_keys.empty?
      @rlsb.add_table_existence_requirement_query(:amenity_park)
      assert @rlsb.valid_query?
      assert !@rlsb.sinter_keys.empty?
    end
  end

  describe "#scoring?" do
    it "must be scoring if scoring query was added" do
      assert !@rlsb.scoring?
      @rlsb.add_table_existence_scoring_query(:amenity_park, 4)
      assert @rlsb.scoring?
    end

    it "must not be affected by adding requirement queries" do
      assert !@rlsb.scoring?
      @rlsb.add_table_existence_requirement_query(:amenity_park)
      assert !@rlsb.scoring?
    end
  end

  describe "#add_table_existence_requirement_query" do
    before do
      @rw.add_boolean_keys(:amenity_park, [1,2,3,4,5,6,7,8,9,10])
      @rw.add_boolean_keys(:amenity_pool, [1,2,3,4,5,100,100000])
    end

    it "must return -1 if query doesn't have requirement query (aka. is invalid_query?)" do
      assert @rlsb.invalid_query?
      assert @rlsb.eval == -1
    end

    it "must return keys of list that match a single requirement" do
      @rlsb.add_table_existence_requirement_query(:amenity_park)
      list_key = @rlsb.eval
      list_values = @rw.get_list(list_key).sort
      assert list_values == [1,2,3,4,5,6,7,8,9,10]
    end

    it "must return keys of list that match many requirements" do
      @rlsb.add_table_existence_requirement_query(:amenity_park)
      @rlsb.add_table_existence_requirement_query(:amenity_pool)
      list_key = @rlsb.eval
      list_values = @rw.get_list(list_key).sort
      assert list_values == [1,2,3,4,5]
    end

    it "must return nothing if nothing matches it" do
      @rlsb.add_table_existence_requirement_query(:amenity_hoa)
      list_key = @rlsb.eval
      list_values = @rw.get_list(list_key).sort
      assert list_values.empty?
    end
  end

  describe "with an existence requirement selected" do
    before do
      @rw.add_boolean_keys(:amenity_park, [1,2,3,4,5,6,7,8,9,10])
      @rlsb.add_table_existence_requirement_query(:amenity_park)
    end

    describe "#add_table_not_exist_requirement_query" do
      it "must return keys of list that match requirements" do
        @rw.add_boolean_keys(:amenity_pool, [1,2,3,4,5,100,100000])

        @rlsb.add_table_not_exist_requirement_query(:amenity_pool)
        list_key = @rlsb.eval
        list_values = @rw.get_list(list_key).sort
        assert list_values == [6,7,8,9,10]
      end

      it "must return nothing if nothing matches it" do
        @rlsb.add_table_not_exist_requirement_query(:amenity_park)
        list_key = @rlsb.eval
        list_values = @rw.get_list(list_key).sort
        assert list_values.empty?
      end
    end

    describe "#add_range_requirement_query" do
      before do
        @rw.set_range_keys(:square_feet, [
                             [1, 5],
                             [2, 6],
                             [3, 9],
                             [4, 10],
                             [5, 20],
                             [6, 25],
                             [7, 5],
                             [8, 9],
                             [9, 20],
                             [10,30]
                           ]);
      end
      it "must return keys of list that match a single range requirement" do
        @rlsb.add_range_requirement_query(:square_feet, 1..5)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key).sort
        assert list_values == [1, 7]
      end

      it "must return keys of list that match many range requirements" do
        @rlsb.add_range_requirement_query(:square_feet,  6..9)    #   2,3,    8
        @rw.set_range_keys(:monthly_rent, [
                             [1, 500],
                             [2, 600],
                             [3, 900],
                             [4, 1000],
                             [5, 2000],
                             [6, 2500],
                             [7, 500],
                             [8, 900],
                             [9, 2000],
                             [10,3000]
                           ]);
        @rlsb.add_range_requirement_query(:monthly_rent, 1..1000) # 1,2,3,4,7,8
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key).sort
        assert list_values == [2, 3, 8]
      end
    end

    describe "#add_table_existence_scoring_query" do
      it "must order the keys of list by a single table existence scoring query" do
        @rw.add_boolean_keys(:amenity_parking_garage, [8,9,10])
        @rlsb.add_table_existence_scoring_query(:amenity_parking_garage, 1)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [1,2,3,4,5,6,7], [8,9,10])
      end

      it "must order the keys of list by multiple table existence scoring queries" do
        @rw.add_boolean_keys(:amenity_parking_garage, [8,9,10])
        @rw.add_boolean_keys(:amenity_hoa, [10])
        @rlsb.add_table_existence_scoring_query(:amenity_parking_garage, 1)
        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 1)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [1,2,3,4,5,6,7], [8,9], [10])
      end
    end

    describe "#add_range_scoring_query" do
      before do
        @rw.set_range_keys(:square_feet, [
                             [1, 5],
                             [2, 6],
                             [3, 9],
                             [4, 10],
                             [5, 20],
                             [6, 25],
                             [7, 5],
                             [8, 9],
                             [9, 20],
                             [10,30]
                           ]);
      end
      it "must order the keys of list by a single range scoring query" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [4,5,6,9,10], [1,2,3,7,8])
      end

      it "must order the keys of list by multiple range scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rw.set_range_keys(:monthly_rent, [
                             [1, 100],
                             [2, 200],
                             [3, 300],
                             [4, 400],
                             [5, 500],
                             [6, 600],
                             [7, 700],
                             [8, 800],
                             [9, 900],
                             [10,1000]
                           ]);
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6,9,10], [4,7,8], [1,2,3])
      end
    end

    describe "#add_table_not_exist_scoring_query" do
      it "must order the keys of the list by a single not_exist scoring query" do
        @rw.add_boolean_keys(:amenity_smoking, [1,2,3,4,5])
        @rlsb.add_table_not_exist_scoring_query(:amenity_smoking)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [1,2,3,4,5], [6,7,8,9,10])
      end

      it "must order the keys of the list by a multiple not_exist scoring query" do
        @rw.add_boolean_keys(:amenity_smoking, [1,2,3,4,5])
        @rw.add_boolean_keys(:amenity_hoa,     [1,2,3,6])
        @rlsb.add_table_not_exist_scoring_query(:amenity_smoking)
        @rlsb.add_table_not_exist_scoring_query(:amenity_hoa)
        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [1,2,3], [5,4,6], [7,8,9,10])
      end
    end

    describe "Test Scoring" do
      before do
        @rw.set_range_keys(:square_feet, [
                             [1, 5], #
                             [2, 6], #
                             [3, 9], #
                             [4, 10],
                             [5, 20],
                             [6, 25],
                             [7, 5], #
                             [8, 9], #
                             [9, 20],
                             [10,30]
                           ]);
        @rw.set_range_keys(:monthly_rent, [
                             [1, 100], #
                             [2, 200], #
                             [3, 300], #
                             [4, 400], #
                             [5, 500],
                             [6, 600],
                             [7, 700],
                             [8, 800],
                             [9, 900],
                             [10,1000]
                           ]);
        @rw.add_boolean_keys(:amenity_hoa, [7])
        @rw.add_boolean_keys(:amenity_smoking, [7,8,9,10])
        @rw.add_boolean_keys(:amenity_dogs_allowed, [8,9,10])
      end

      it "should handle combined scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)
        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 1)

        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6,9,10], [4,8], [1,2,3,7])
      end

      it "should handle combined scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)
        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 2)

        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6,9,10], [4,8], [1,2,3], [7])
      end

      it "should handle combined scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)
        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 1)
        @rlsb.add_table_existence_scoring_query(:amenity_dogs_allowed, 1)

        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6], [9,10,4], [8,1,2,3,7])
      end

      it "should handle combined scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)
        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 1)
        @rlsb.add_table_existence_scoring_query(:amenity_dogs_allowed, 2)

        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6], [4], [9,10,1,2,3,7], [8])
      end

      it "should handle combined scoring queries" do
        @rlsb.add_range_scoring_query(:square_feet, 1...10)
        @rlsb.add_range_scoring_query(:monthly_rent, 100..400)

        @rlsb.add_table_existence_scoring_query(:amenity_hoa, 2)
        @rlsb.add_table_existence_scoring_query(:amenity_dogs_allowed, 2)

        @rlsb.add_table_not_exist_scoring_query(:amenity_smoking, 2)

        list_key    = @rlsb.eval
        list_values = @rw.get_list(list_key)
        assert array_sections_equal(list_values, [5,6,9,10], [7,8], [1,2,3,4])
      end
    end
  end
end
