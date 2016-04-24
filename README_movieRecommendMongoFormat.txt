# Database name
movieRecommend

# Collections
1. actors_list
2. anew
3. genres_list
4. movie
5. tag
6. user_profiles
7. user_rate

1. actors_list
# actors_list sample (popular means popularity, mid means movielens movie id)
{ 
    "_id" : ObjectId("571bcb18a5b770040425903d"), 
    "popular" : 1, 
    "actor" : "Daniel Kountz", 
    "relevant_movie" : [ 
        { 
            "imdb_votes" : "4,002", 
            "imdb_rating" : "6.5", 
            "mid" : 125972 
        } 
    ] 
}

# actors_list special case (top 100 most popular actors)
> db.actors_list.find({"actor": "all"})
{ 
    "_id" : ObjectId("571bcb4ab94ea65712794ce3"), 
    "actor" : "all", 
    "most_popular" : [ 
        "John Wayne", 
        "Gérard Depardieu", 
        "Michael Caine", 
        ... 
    ] 
}

2. anew
# anew -> Affective Norms for English Words, no need to know about its structure, almost the same as the csv file

3. genres_list
# genres_list sample (popular means popularity, most_popular and top_rated store the movielens movie id)
{ 
    "_id" : ObjectId("57193d64ce1d02063d638937"), 
    "genre" : "Romance", 
    "popular" : 5056, 
    "relevant_movie" : [ 
        { 
            "mid" : 3, 
            "imdb_votes" : "18,747", 
            "imdb_rating" : "6.6" 
        }, 
        { 
            "mid" : 4, 
            "imdb_votes" : "7,265", 
            "imdb_rating" : "5.6" 
        }, 
        ...
    ], 
    "top_rated" : [ 
        7669, 
        86055, 
        93988, 
        ... 
    ], 
    "most_popular" : [ 
        356, 
        1721, 
        7361, 
        ... 
    ] 
}

# genres_list special case (top 100 most popular and highest rated movies)
> db.genres_list.find({"genre": "all"})
{ 
    "_id" : ObjectId("571a33b6ce1d02063d63c08d"), 
    "genre" : "all", 
    "top_rated" : [ 
        7502, 
        102976, 
        86057, 
        ... 
    ], 
    "most_popular" : [ 
        318, 
        58559, 
        79132, 
        ... 
    ] 
}

4. movie
# movie sample
{ 
    "_id" : ObjectId("5717aceda5b770473c5975d6"), 
    "mid" : 1, 
    "imdbid" : 114709, 
    "title" : "Toy Story (1995)", 
    "popular" : 53059, 
    "tags" : [ 
        "10,0.563", 
        "28,0.894", 
        "29,0.699", 
        ... 
    ], 
    "genres" : [ 
        "Animation", 
        "Adventure", 
        "Comedy" 
    ], 
    "year" : "1995", 
    "country" : "USA", 
    "language" : "English", 
    "poster" : "http://ia.media-imdb.com/images/M/MV5BMTgwMjI4MzU5N15BMl5BanBnXkFtZTcwMTMyNTk3OA@@._V1_SX300.jpg", 
    "type" : "movie", 
    "runtime" : "81 min", 
    "plot" : "A cowboy doll is profoundly threatened and jealous when a new spaceman figure supplants him as top toy in a boy's room.", 
    "metascore" : "92", 
    "rated" : "G", 
    "imdb_rating" : "8.3", 
    "imdb_votes" : "599,356", 
    "director" : "John Lasseter", 
    "actors" : [ 
        "Tom Hanks", 
        "Tim Allen", 
        "Don Rickles", 
        "Jim Varney" 
    ], 
    "writer" : "John Lasseter (original story by), Pete Docter (original story by), Andrew Stanton (original story by), Joe Ranft (original story by), Joss Whedon (screenplay), Andrew Stanton (screenplay), Joel Cohen (screenplay), Alec Sokolow (screenplay)", 
    "similar_movies" : [ 
        6377, 
        4886, 
        68954, 
        ... 
    ] 
}

5. tag
# tag sample ("relevant_movie": "tag_id,relevant_score")
{
    "_id" : ObjectId("5717acf3a5b770473c59fb76"),
    "popular" : 61,
    "tid" : 0,
    "content" : "007",
    "relevant_movie" : [
        "10,1.0",
        "380,0.512",
        "1479,0.563",
        ...
    ]
}

6. user_profiles
# user_profiles sample
{ 
    "_id" : ObjectId("571542a4a5b77029ecafdc1e"), 
    "lang" : "en", 
    "screen_name" : "BrunoMars", 
    "extracted_tweets" : [ 
        "LETS CELBRATE!! Official after party one oak!! NY!! LETS GO!!", 
        ...
    ]
    "suggested_categories" : [ 
        "sports", 
        "entertainment", 
        "music", "digital-creators", 
        ...
    ], 
    "followers_count" : 25062418, 
    "friends_count" : 109, 
    "extracted_users" : [ 
        [ 
            "iHeartRadio", 
            "iHeartRadio" 
        ], 
        [ 
            "LeoDiCaprio", 
            "Leonardo DiCaprio" 
        ], 
        [ 
            "holyfield", 
            "Evander Holyfield" 
        ],
        ...
    ]
    "location" : "Los Angeles, CA",
    "extracted_tags" : [
        "BringIt",
        "WishIwroteThat",
        "Rest",
        ...
    ],
    "statuses_count" : 3525,
    "name" : "Bruno Mars",
    "profile_image_url" : "http://pbs.twimg.com/profile_images/531626153837359105/ZR11lw1R_normal.jpeg",
    "id" : 100220864
}

7. user_rate (user ratings from MovieLens)
# user_rate sample ("ratings": [movie id, rating])
{
    "_id" : ObjectId("571a79a1a5b77021b8992339"),
    "uid" : 1,
    "ratings" : [
        [
            169,
            2.5
        ],
        [
            2471,
            3
        ],
        [
            48516,
            5
        ]
    ]
}
