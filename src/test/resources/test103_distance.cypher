
WITH point({ longitude: {from_longitude}, latitude: {from_latitude} }) AS fromPoint, point({ longitude: {to_longitude}, latitude: {to_latitude} }) AS toPoint
RETURN round(distance(fromPoint, toPoint)) AS distance


