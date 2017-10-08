CALL spatial.withinDistance({layer},{lat:57.716043,lon:11.971407}, 1.5d) yield node as n 
WITH n 
RETURN  n.name,n.bbox,n.geometry,n.entitytype 


 



