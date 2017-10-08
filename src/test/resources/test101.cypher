CALL spatial.withinDistance({layer},{lat:{latitude},lon:{longitude}}, {distanceInKm}) yield node as n 
WITH n 
RETURN  n.name,n.bbox,n.geometry,n.entitytype

