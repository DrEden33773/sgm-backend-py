MATCH
    (person:Person)-[a:knows]->(otherPerson:Person),
    (otherPerson)-[b:isLocatedIn]->(locationCity:City),
    (otherPerson)-[c:workAt]->(company:Company),
    (company)-[d:isLocatedIn]->(companyCountry:Country),
    (otherPerson)-[e:studyAt]->(university:University),
    (university)-[f:isLocatedIn]->(universityCity:City)
WHERE
    person.id <> otherPerson.id AND
    person.id = 6597069766786 AND
    otherPerson.firstName = "Jose" 
RETURN person, otherPerson, locationCity, company, companyCountry, university, universityCity, a, b, c, d, e, f
// LIMIT 50