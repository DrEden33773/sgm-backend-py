MATCH (person:Person)-[a:knows]->(otherPerson:Person),
      (otherPerson)-[b:isLocatedIn]->(locationCity:City)
OPTIONAL MATCH (otherPerson)-[c:workAt]->(company:Company),
             (company)-[d:isLocatedIn]->(companyCountry:Country)
OPTIONAL MATCH (otherPerson)-[e:studyAt]->(university:University),
             (university)-[f:isLocatedIn]->(universityCity:City)
RETURN person, otherPerson, locationCity, company, companyCountry, university, universityCity, a, b, c, d, e, f
LIMIT 50