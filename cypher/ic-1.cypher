MATCH
    (person:Person)-[a:knows]->(otherPerson:Person),
    (otherPerson)-[b:isLocatedIn]->(locationCity:City),
    (otherPerson)-[c:workAt]->(company:Company),
    (company)-[d:isLocatedIn]->(companyCountry:Country),
    (otherPerson)-[e:studyAt]->(university:University),
    (university)-[f:isLocatedIn]->(universityCity:City)
WHERE
    person <> otherPerson
RETURN person, otherPerson, locationCity, company, companyCountry, university, universityCity, a, b, c, d, e, f
LIMIT 50