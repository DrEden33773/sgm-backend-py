MATCH
    (person:Person)-[a:knows]->(otherPerson:Person),
    (otherPerson)-[b:workAt]->(company:Company),
    (company)-[c:isLocatedIn]->(country:Country)
WHERE
    person.id <> otherPerson.id AND
    person.id = 6597069766786 AND
    otherPerson.id > 10995116277900 AND
    country.name = "Pakistan"
RETURN person, a, otherPerson, b, company, c, country
// LIMIT 10