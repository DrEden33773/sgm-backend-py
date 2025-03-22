MATCH 
    path = (person:Person)-[:knows]->(otherPerson:Person)-[:isLocatedIn]->(locationCity:City)
WITH 
    person, otherPerson, locationCity, path as p1
MATCH 
    path2 = (otherPerson)-[:workAt]->(company:Company)-[:isLocatedIn]->(companyCountry:Country)
WITH
    person, otherPerson, locationCity, company, companyCountry, p1, path2 as p2
MATCH
    path3 = (otherPerson)-[:studyAt]->(university:University)-[:isLocatedIn]->(universityCity:City)
WHERE 
    person <> otherPerson
RETURN p1, p2, path3
LIMIT 50