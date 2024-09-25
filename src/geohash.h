/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef GEOHASH_H_
#define GEOHASH_H_

#include <stddef.h>
#include <stdint.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

#define HASHISZERO(r) (!(r).bits && !(r).step)
#define RANGEISZERO(r) (!(r).max && !(r).min)
#define RANGEPISZERO(r) (r == NULL || RANGEISZERO(*r))

#define GEO_STEP_MAX 26 /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
#define GEO_LAT_MIN -85.05112878
#define GEO_LAT_MAX 85.05112878
#define GEO_LONG_MIN -180
#define GEO_LONG_MAX 180

typedef enum {
    GEOHASH_NORTH = 0,
    GEOHASH_EAST,
    GEOHASH_WEST,
    GEOHASH_SOUTH,
    GEOHASH_SOUTH_WEST,
    GEOHASH_SOUTH_EAST,
    GEOHASH_NORT_WEST,
    GEOHASH_NORT_EAST
} GeoDirection;

typedef struct {
    uint64_t bits;
    uint8_t step;
} GeoHashBits;

typedef struct {
    double min;
    double max;
} GeoHashRange;

typedef struct {
    GeoHashBits hash;
    GeoHashRange longitude;
    GeoHashRange latitude;
} GeoHashArea;

typedef struct {
    GeoHashBits north;
    GeoHashBits east;
    GeoHashBits west;
    GeoHashBits south;
    GeoHashBits north_east;
    GeoHashBits south_east;
    GeoHashBits north_west;
    GeoHashBits south_west;
} GeoHashNeighbors;

#define CIRCULAR_TYPE 1
#define RECTANGLE_TYPE 2
typedef struct {
    int type; /* search type */
    double xy[2]; /* search center point, xy[0]: lon, xy[1]: lat */
    double conversion; /* km: 1000 */
    double bounds[4]; /* bounds[0]: min_lon, bounds[1]: min_lat
                       * bounds[2]: max_lon, bounds[3]: max_lat */
    union {
        /* CIRCULAR_TYPE */
        double radius;
        /* RECTANGLE_TYPE */
        struct {
            double height;
            double width;
        } r;
    } t;
} GeoShape;

/*
 * 0:success
 * -1:failed
 */
void geohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range);
int geohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range,
                  double longitude, double latitude, uint8_t step,
                  GeoHashBits *hash);
int geohashEncodeType(double longitude, double latitude,
                      uint8_t step, GeoHashBits *hash);
int geohashEncodeWGS84(double longitude, double latitude, uint8_t step,
                       GeoHashBits *hash);
int geohashDecode(const GeoHashRange long_range, const GeoHashRange lat_range,
                  const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeType(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeWGS84(const GeoHashBits hash, GeoHashArea *area);
int geohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy);
int geohashDecodeToLongLatType(const GeoHashBits hash, double *xy);
int geohashDecodeToLongLatWGS84(const GeoHashBits hash, double *xy);
int geohashDecodeToLongLatMercator(const GeoHashBits hash, double *xy);
void geohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors);

#if defined(__cplusplus)
}
#endif
#endif /* GEOHASH_H_ */
