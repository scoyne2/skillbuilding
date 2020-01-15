from pyspark.sql.types import *

hashtags_schema  = StructType([StructField("indices", ArrayType(IntegerType(), True)),
                               StructField("text", StringType(), True)
                               ])

urls_schema = StructType([StructField("url", StringType()),
                          StructField("indices", ArrayType(IntegerType(), True)),
                          StructField("id_str", StringType()),
                          StructField("expanded_url", StringType()),
                          StructField("display_url", StringType()) 
                         ])

user_mentions_schema = StructType([StructField("id", StringType()),
                                   StructField("indices", ArrayType(IntegerType(), True)),
                                   StructField("id_str", StringType()),
                                   StructField("screen_name", StringType()),
                                   StructField("name", StringType()) 
                                   ])

media_schema  = StructType([StructField("display_url", StringType(), True),
                            StructField("expanded_url", StringType(), True),
                            StructField("id", StringType(), True),
                            StructField("id_str", StringType(), True),
                            StructField("indices", ArrayType(IntegerType(), True)),
                            StructField("media_url", StringType(), True),
                            StructField("media_url_https", StringType(), True),
                            StructField("sizes", StringType(), True), #expand to a size object
                            StructField("source_status_id_str", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("url", StringType(), True)
                           ])

symbols_schema  = StructType([StructField("indices", ArrayType(IntegerType(), True)),
                              StructField("text", StringType(), True)
                             ])

polls_schema  = StructType([StructField("options", ArrayType(StringType(), True),True), #expand out to an array of Options Objects
                            StructField("end_datetime", StringType(), True),
                            StructField("duration_minutes", StringType(), True)
                           ])

entities_schema = StructType([
                              StructField("hashtags", ArrayType(hashtags_schema, True), True),
                              StructField("urls", ArrayType(urls_schema, True), True),
                              StructField("user_mentions", ArrayType(user_mentions_schema, True), True),
                              StructField("media", ArrayType(media_schema, True), True),
                              StructField("symbols", ArrayType(symbols_schema, True), True),
                              StructField("polls", ArrayType(polls_schema, True), True)                                                
                             ])
 
 
kinesis_schema = StructType([
    StructField("quote_count", StringType(), True),
    StructField("contributors", StringType(), True),
    StructField("truncated", StringType(), True),
    StructField("text", StringType(), True),
    StructField("is_quote_status", StringType(), True),
    StructField("in_reply_to_status_id", StringType(), True),
    StructField("reply_count", StringType(), True),
    StructField("id", StringType(), True),
    StructField("favorite_count", StringType(), True),
    StructField("entities", entities_schema, True),
                          

])


# extra = StructType([
#     StructField("retweeted", StringType()),
#     StructField("coordinates", StringType()),
#     StructField("timestamp_ms", StringType()),
#     StructField("source", StringType()),
#     StructField("in_reply_to_screen_name", StringType()),
#     StructField("id_str", StringType()),
#     StructField("display_text_range", ArrayType(StringType(), True)),
#     StructField("retweet_count", StringType()),
#     StructField("in_reply_to_user_id", StringType()),
#     StructField("favorited", StringType()),
#     StructField("user",  StructType([
#                                     StructField("follow_request_sent", StringType()),
#                                     StructField("profile_use_background_image", StringType()),
#                                     StructField("default_profile_image", StringType()),
#                                     StructField("id", StringType()),
#                                     StructField("default_profile", StringType()),
#                                     StructField("verified", StringType()),
#                                     StructField("profile_image_url_https", StringType()),
#                                     StructField("profile_sidebar_fill_color", StringType()),
#                                     StructField("profile_text_color", StringType()),
#                                     StructField("profile_text_color", StringType()),
#                                     StructField("followers_count", StringType()),
#                                     StructField("profile_sidebar_border_color", StringType()),
#                                     StructField("id_str", StringType()),
#                                     StructField("profile_background_color", StringType()),
#                                     StructField("listed_count", StringType()),
#                                     StructField("profile_background_image_url_https", StringType()),
#                                     StructField("utc_offset", StringType()),
#                                     StructField("statuses_count", StringType()),
#                                     StructField("description", StringType()),
#                                     StructField("friends_count", StringType()),
#                                     StructField("location", StringType()),
#                                     StructField("profile_link_color", StringType()),
#                                     StructField("profile_image_url", StringType()),
#                                     StructField("following", StringType()),
#                                     StructField("geo_enabled", StringType()),
#                                     StructField("profile_background_image_url", StringType()),
#                                     StructField("name", StringType()),
#                                     StructField("lang", StringType()),
#                                     StructField("profile_background_tile", StringType()),
#                                     StructField("favourites_count", StringType()),
#                                     StructField("screen_name", StringType()),
#                                     StructField("notifications", StringType()),
#                                     StructField("url", StringType()),
#                                     StructField("created_at", StringType()),
#                                     StructField("contributors_enabled", StringType()),
#                                     StructField("time_zone", StringType()),
#                                     StructField("protected", StringType()),
#                                     StructField("translator_type", StringType()),
#                                     StructField("is_translator", StringType()),
#                                     ])),
#     StructField("geo", StructType([
#                         StructField("type", StringType()),
#                         StructField("coordinates", ArrayType(DecimalType(), True))
#                         ])),
#     StructField("in_reply_to_user_id_str", StringType()),
#     StructField("possibly_sensitive", StringType()),
#     StructField("lang", StringType()),
#     StructField("created_at", StringType()),
#     StructField("filter_level", StringType()),
#     StructField("in_reply_to_status_id_str", StringType()),
#     StructField("place", StructType([
#                         StructField("full_name", StringType()),
#                         StructField("url", StringType()),
#                         StructField("country", StringType()),
#                         StructField("place_type", StringType()),
#                         StructField("bounding_box", 
#                                     StructType([StructField("type", StringType()),
#                                     StructField("coordinates", ArrayType(ArrayType(DecimalType(), True), True))
#                                       ])),
#                         StructField("attributes", StringType()),
#                         StructField("id", StringType()),
#                         StructField("name", StringType()),
#                         ]))
