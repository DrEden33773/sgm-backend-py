// Q6. Tag co-occurrence

MATCH (knownTag:Tag { name: "Carl_Gustaf_Emil_Mannerheim" })
WITH knownTag.id as knownTagId

MATCH (person:Person { id: 4398046511333 })-[:knows]-(friend)
WHERE NOT person=friend
WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[:hasCreator]-(post:Post),
          (post)-[:hasTag]->(t:Tag{id: knownTagId}),
          (post)-[:hasTag]->(tag:Tag)
    WHERE NOT t = tag
    WITH
        tag.name as tagName,
        count(post) as postCount
RETURN
    tagName,
    postCount
ORDER BY
    postCount DESC,
    tagName ASC
LIMIT 10