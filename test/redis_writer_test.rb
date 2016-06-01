require 'minitest/autorun'
require '../lib/redis_writer'

describe RedisWriter do
  before(:each) do
    @rw = RedisWriter.new
    @rw.force_db_reset
  end

  it "should be empty" do
    assert @rw.dbsize == 0
  end

  describe "#add_boolean_key, #get_boolean_key" do
    it "should increase the number of keys" do
      @rw.add_boolean_keys(:amenity_parking_garage, [1, 2, 3, 4 ,5])
      assert @rw.dbsize == 1

      @rw.add_boolean_keys(:amenity_parking_garage, [9, 10, 11, 12])
      assert @rw.dbsize == 1

      @rw.add_boolean_keys(:amenity_hoa, [1, 2, 3, 4 ,5])
      assert @rw.dbsize == 2
    end

    it "should add new keys" do
      @rw.add_boolean_keys(:amenity_parking_garage, [1, 2, 3, 4 ,5])
      assert @rw.get_boolean_keys(:amenity_parking_garage) == [1, 2, 3, 4, 5]
    end

    it "should keep old and new keys" do
      @rw.add_boolean_keys(:amenity_parking_garage, [1, 2, 3, 4 ,5])
      @rw.add_boolean_keys(:amenity_parking_garage, [9, 10, 11, 12])
      assert @rw.get_boolean_keys(:amenity_parking_garage) == [1, 2, 3, 4, 5, 9, 10, 11, 12]
    end
  end

  describe "#set_range_key, #get_range_key" do
    it "should increase the number of keys" do
      @rw.set_range_keys(:square_feet, [[1, 3], [2, 3], [3, 3], [3, 5]])
      assert @rw.dbsize == 1
      @rw.set_range_keys(:square_feet, [[4, 3], [5, 3], [6, 3], [7, 5]])
      assert @rw.dbsize == 1

      @rw.set_range_keys(:monthly_rent, [[8, 3], [9, 3], [10, 3], [11, 5]])
      assert @rw.dbsize == 2
    end

    it "should return the values" do
      @rw.set_range_keys(:square_feet, [[1, 3], [2, 3], [3, 3], [4, 5]])
      ids = @rw.get_range_keys(:square_feet, 3..5).sort
      assert ids == [[1, 3], [2, 3], [3, 3], [4, 5]].sort
    end

    it "should overwrite old values" do
      @rw.set_range_keys(:square_feet, [[1, 3], [2, 3], [3, 3], [4, 5]])
      ids = @rw.get_range_keys(:square_feet, 3..5).map(&:first).sort
      assert ids == [1, 2, 3, 4]

      # Change id 4 to be out of the range
      @rw.set_range_keys(:square_feet, [[4, 6]])
      ids = @rw.get_range_keys(:square_feet, 3..5).map(&:first).sort
      assert ids == [1, 2, 3]
    end
  end
  # geospatial data
end
