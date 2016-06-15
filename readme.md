# Redis Database 'Schema'

### Tables

Only 4 types of tables would exist.
1. Boolean Values (for existence)
2. Integer Values (for ranges)
3. Geospatial values
4. Home JSON hashes

#### Boolean Values

String attributes can just be normal key:value
Key:  `#{type_of_attribute}_#{Attribute}`
Value: `Set(id of homes)`

Example 1:
`style_garden`: `{1, 2, 4, 5}`
Example 2:
`Amenity:Gas`: `{8, 9, 23}`

##### Proposed Changes

* Change this to use the bitset operations. This will save us space and potentially be much faster - the later phases of the algorithm could also use bitsets, however, the algorithm would have to be slightly revised if we used bitsets for the entire thing.
  - http://redis.io/topics/memory-optimization#bit-and-byte-level-operations


#### Integer Values (for range queries)

Integer attributes should go into a hash so they can be accessed quickly:
Key:   `square_feet`
Hash Keys: `#{id}`
Value: `JSON hash`
ex.

```
square_feet: {
    1: 800
    2: 930
    ....
}
```

##### Proposed Changes

* It's possible that using a sorted set 'could' be faster...But it's difficult to do operations with a sorted set.


#### Geospatial coordinates

This is a built in data type, but, it still uses the key and value features.
Key:   `Id of home at location`
Value: `Lat, Long`

#### Home Hashes
Key:       `home_#{id}`
Hash Keys: `Still to be determined. Based on what we would be storing in the cms`
Value:     `JSON hash`
ex.
```
home_32: {
    bedrooms:        7
    half_baths:      8
    smoking_allowed: true
    ...
}
home_64: {
    bedrooms:        7
    half_baths:      8
    <!-- Save space by just not setting the key to false -->
    ...
}
```
