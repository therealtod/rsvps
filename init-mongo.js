db.createUser(
    {
        user: "empathy",
        pwd: "empathy",
        roles : [
            {
                role : "readWrite",
                db : "rsvps"
            }
        ]
    }
)