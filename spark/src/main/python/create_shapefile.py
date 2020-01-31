from osgeo import ogr
import sys

def create_polygon(coords):          
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for coord in coords:
        ring.AddPoint(coord[0], coord[1])

    # Create polygon
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly.ExportToWkt()

def write_shapefile(poly, out_shp):
    """
    https://gis.stackexchange.com/a/52708/8104
    """
    # Now convert it to a shapefile with OGR    
    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource(out_shp)
    layer = ds.CreateLayer('', None, ogr.wkbPolygon)
    # Add one attribute
    layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn('GEOID', ogr.OFTString))
    defn = layer.GetLayerDefn()

    ## If there are multiple geometries, put the "for" loop here

    # Create a new feature (attribute and geometry)
    feat = ogr.Feature(defn)
    feat.SetField('id', 123)
    feat.SetField('GEOID', "0")

    # Make a geometry, from Shapely object
    geom = ogr.CreateGeometryFromWkt(poly)
    feat.SetGeometry(geom)

    layer.CreateFeature(feat)
    feat = geom = None  # destroy these

    # Save and close everything
    ds = layer = feat = geom = None

x0 = 0
x1 = 10000000
y0 = -10000000
y1 = 0

coords = [(x0, y0), (x0, y1), (x1, y1), (x1, y0)]
out_shp = sys.argv[1]
poly = create_polygon(coords)
write_shapefile(poly, out_shp)

