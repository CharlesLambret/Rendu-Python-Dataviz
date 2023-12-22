import pymongo
import pandas
from bson.son import SON
from datetime import datetime
import matplotlib.pyplot as plt
from bson.decimal128 import Decimal128
import numpy as np
import seaborn as sns



#--------------------------------------------------------------Connexion à la DB--------------------------------------------------------#

URI = 'mongodb+srv://CharlesLambret:tIL3Zy0NhwLxW0FnJp1j@cluster0.kcse7rg.mongodb.net/'
client = pymongo.MongoClient(URI) 

dbrestaurants = client.sample_restaurants
restaurants = dbrestaurants.restaurants 


dbairbnb = client.sample_airbnb
airbnb = dbairbnb.listingsAndReviews

#--------------------------------------------------------------Restaurants--------------------------------------------------------#


# 1. Lister tous les restaurants de la chaîne Bareburger (rue, quartier)

findBareBurgerRestaurants = pandas.DataFrame(
        restaurants.find(
                { "name": "Bareburger" }, 
                { "address.street": 1, "borough": 1 }
    )
)

#print("Restaurants de la chaîne Bareburger", findBareBurgerRestaurants)
"""
Restaurants de la chaîne Barburger                          _id                         address    borough
0   5eb3d669b31de5d588f45084         {'street': '31 Avenue'}     Queens
1   5eb3d669b31de5d588f45882   {'street': 'Laguardia Place'}  Manhattan
2   5eb3d669b31de5d588f46007          {'street': '7 Avenue'}   Brooklyn
3   5eb3d669b31de5d588f461ff         {'street': '31 Street'}     Queens
4   5eb3d669b31de5d588f46363          {'street': '8 Avenue'}  Manhattan
5   5eb3d669b31de5d588f46470          {'street': '2 Avenue'}  Manhattan
6   5eb3d669b31de5d588f4658c     {'street': 'Austin Street'}     Queens
7   5eb3d669b31de5d588f46891    {'street': 'Bell Boulevard'}     Queens
8   5eb3d669b31de5d588f470e8          {'street': 'Court St'}   Brooklyn
9   5eb3d669b31de5d588f47375  {'street': 'West   46 Street'}  Manhattan
10  5eb3d669b31de5d588f47409          {'street': '1 Avenue'}  Manhattan
11  5eb3d669b31de5d588f486b2        {'street': 'William St'}  Manhattan

"""


#----------------------------------------------------------------------#
# 2. Lister les trois chaînes de restaurant les plus présentes

popularRestaurantspipeline = [
    {"$group": {"_id": "$name", "count": {"$sum": 1}}},
    {"$sort": SON([("count", -1)])},
    {"$limit": 3}
]

mostpopularRestaurants = pandas.DataFrame(
    restaurants.aggregate(popularRestaurantspipeline)
    )

#print("Trois chaînes de restaurants les plus présentes",mostpopularRestaurants)

"""
Trois chaînes de restaurants les plus présentes                 _id  count
0            Subway    421
1  Starbucks Coffee    223
2        Mcdonald'S    208

"""


#----------------------------------------------------------------------#
# 3. Donner les 10 styles de cuisine les plus présents dans la collection

CookingStylespipeline = [
    {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
    {"$sort": SON([("count", -1)])},
    {"$limit": 10}
]

CookingStyleResult = pandas.DataFrame(
    restaurants.aggregate(CookingStylespipeline)
    )

#print("10 styles de cuisine les plus présents",CookingStyleResult)

"""
10 styles de cuisine les plus présents                                                  _id  count
0                                           American   6183
1                                            Chinese   2418
2                                    Café/Coffee/Tea   1214
3                                              Pizza   1163
4                                            Italian   1069
5                                              Other   1011
6  Latin (Cuban, Dominican, Puerto Rican, South &...    850
7                                           Japanese    760
8                                            Mexican    754
9                                             Bakery    691

"""


#----------------------------------------------------------------------#
# 4. Lister les 10 restaurants les mieux notés (note moyenne la plus haute)

TopGradesPipeline = [
    {"$unwind": "$grades"},
    {"$match": {
        "grades.score": {"$exists": True, "$ne": None, "$gte": 0}  
    }},
    {"$group": {
        "_id": "$_id",
        "name": {"$first": "$name"},
        "average_score": {"$avg": "$grades.score"} 
    }},
    {"$sort": {"average_score": -1}},  
    {"$limit": 10}  
]

topGradesResults = pandas.DataFrame(
    restaurants.aggregate(TopGradesPipeline)
)

#print("10 restaurants les mieux notés", topGradesResults)


"""
0  5eb3d669b31de5d588f48751      Juice It Health Bar           75.0
1  5eb3d669b31de5d588f4897e    Golden Dragon Cuisine           73.0
2  5eb3d669b31de5d588f4866b      Palombo Pastry Shop           69.0
3  5eb3d669b31de5d588f48b4a  Chelsea'S Juice Factory           69.0
4  5eb3d669b31de5d588f46ddf              Go Go Curry           65.0
5  5eb3d669b31de5d588f48944       K & D Internet Inc           61.0
6  5eb3d669b31de5d588f4820b                    Koyla           61.0
7  5eb3d669b31de5d588f48467         Ivory D O S  Inc           60.0
8  5eb3d669b31de5d588f4822e                Aki Sushi           60.0
9  5eb3d669b31de5d588f48b1b      Ab Halal Restaurant           58.0
"""

#----------------------------------------------------------------------#
# 5. Lister par quartier le nombre de restaurants, le score moyen et le pourcentage moyen d'évaluation A
pipeline = [
    {"$unwind": "$grades"},
    {"$group": {
        "_id": "$borough",
        "nombre_de_restaurants": {"$sum": 1},
        "score_moyen": {"$avg": "$grades.score"},
        "pourcentage_A": {
            "$avg": {
                "$cond": [{"$eq": ["$grades.grade", "A"]}, 1, 0]
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "borough": "$_id",
        "nombre_de_restaurants": 1,
        "score_moyen": 1,
        "pourcentage_A": {"$multiply": ["$pourcentage_A", 100]}  # Conversion en pourcentage
    }}
]

BoroughsStats = pandas.DataFrame(
    restaurants.aggregate(pipeline)
)

#print(BoroughsStats)

"""
   nombre_de_restaurants  score_moyen        borough  pourcentage_A
0                   3216    11.370958  Staten Island      82.027363
1                   8706    11.036186          Bronx      80.657018
2                  20877    11.634865         Queens      78.660727
3                  21963    11.447976       Brooklyn      78.878113
4                  38622    11.418151      Manhattan      80.718243
5                     79     9.632911        Missing      94.936709

"""

#-----------------------------------------------------------------------------------------------------------------------------#
### Questions complémentaires

# 1. Lister les restaurants (nom et rue uniquement) situés sur une rue ayant le terme Union dans le nom

searchUnion = {
    "address.street": {"$regex": "Union", "$options": "i"} 
}

Unionprojection = {
    "_id": 0,
    "name": 1,
    "address.street": 1
}

restaurants_union_street = pandas.DataFrame(
    restaurants.find(searchUnion, Unionprojection)
)

#print(restaurants_union_street)

"""

                          address                               name
0    {'street': 'Union Turnpike'}                King Yum Restaurant
1    {'street': 'Union Turnpike'}                            P.J.' S
2      {'street': 'Union Street'}            Ferdinando'S Restaurant
3    {'street': 'Unionport Road'}                       Venice Pizza
4    {'street': 'Union Turnpike'}                 Luigi'S Restaurant
..                            ...                                ...
211        {'street': 'Union St'}            Great East Chinese Food
212      {'street': 'Union Tpke'}
213      {'street': 'Union Tpke'}      Marcella'S Pizza And Catering
214      {'street': 'Union Tpke'}  Nikko Hibachi Steakhouse & Lounge
215        {'street': 'Union St'}                Superwings & Things

[216 rows x 2 columns]

"""

#----------------------------------------------------------------------#
# 2. Lister les restaurants ayant eu une visite le 1er février 2014

searchTime = {
    "grades.date": datetime(2014, 2, 1)
}
Timeprojection = {
    "_id": 0,
    "name": 1,
    "address.street": 1,
    "address.zipcode": 1,
}

restaurants_visited_on_february = pandas.DataFrame(
    restaurants.find(searchTime, Timeprojection)
)

#print(restaurants_visited_on_february)

"""
                                              address                                       name
0   {'street': 'East 204 Street', 'zipcode': '10467'}                                 Mcdonald'S
1   {'street': 'Crossbay Boulevard', 'zipcode': '1...                                 Gold'S Gym
2      {'street': 'Burke Avenue', 'zipcode': '10467'}                           K & Q Restaruant
3     {'street': 'Kings Highway', 'zipcode': '11223'}                            Knapp Pizza Iii
4    {'street': 'Seaview Avenue', 'zipcode': '11236'}                Win Hing Chinese Restaurant
5         {'street': '41 Avenue', 'zipcode': '11355'}                   Chinese House Restaurant
6   {'street': 'Coney Island Avenue', 'zipcode': '...                                       Sake
7   {'street': 'Roosevelt Avenue', 'zipcode': '113...                       Roosevelt Sports Bar
8    {'street': 'Forbell Street', 'zipcode': '11256'}  Forbell Cafe (U.S. Post Office Cafeteria)
9   {'street': 'West   44 Street', 'zipcode': '100...                                Shake Shack
10  {'street': 'Roosevelt Avenue', 'zipcode': '113...                        New Flushing Bakery
11  {'street': 'Roosevelt Avenue', 'zipcode': '113...                                Haagen Dazs
12      {'street': 'Main Street', 'zipcode': '11355'}                              First Hot Pot
13  {'street': 'Queens Boulevard', 'zipcode': '113...                             Dunkin' Donuts
14        {'street': '86 Street', 'zipcode': '11223'}                         The Lights Of Baku
15        {'street': '95 Street', 'zipcode': '11372'}                       Emily Bar Restaurant
16   {'street': 'Catalpa Avenue', 'zipcode': '11385'}                                    Norma'S
17         {'street': 'Broadway', 'zipcode': '11373'}              Dunkin Donuts, Baskin Robbins
18     {'street': 'Burke Avenue', 'zipcode': '10467'}             Una Nueva Esperanza/ Herbalife
19        {'street': '41 Avenue', 'zipcode': '11355'}                              Hot Pot House
20        {'street': '39 Avenue', 'zipcode': '11354'}                             Cake House Win
21  {'street': 'Roosevelt Avenue', 'zipcode': '113...                              Dae Jang Geum
22  {'street': 'White Plains Road', 'zipcode': '10...                      Kennedy Fried Chicken
23    {'street': 'Northern Blvd', 'zipcode': '11354'}                                        Kfc
"""
#----------------------------------------------------------------------#
# 3. Lister les restaurants situés entre les longitudes -74.2 et -74.1 et les latitudes 40.1 et 40.2
searchCoordinates = {
    "$or": [
        {"address.coord.0": {"$gte": -74.2, "$lte": -74.1}},  
        {"address.coord.1": {"$gte": 40.1, "$lte": 40.2}}    
    ]
}

CoordinatesProjection = {
    "_id": 0,
    "name": 1,
    "address.coord": 1
}

restaurants_on_location = pandas.DataFrame(
    restaurants.find(searchCoordinates, CoordinatesProjection)
)

#print(restaurants_on_location)

"""
    address                       name
0           {'coord': [-74.1377286, 40.6119572]}              Kosher Island
1           {'coord': [-74.1459332, 40.6103714]}              Bagels N Buns
2    {'coord': [-74.15235919999999, 40.5563756]}  B & M Hot Bagel & Grocery
3           {'coord': [-74.1178949, 40.5734906]}        Plaza Bagels & Deli
4             {'coord': [-74.138263, 40.546681]}     Great Kills Yacht Club
..                                           ...                        ...
626         {'coord': [-74.1033838, 40.5760758]}               Ciminna Cafe
627  {'coord': [-74.1102822, 40.57088419999999]}
628  {'coord': [-74.16969230000001, 40.5602526]}               Jimmy John'S
629         {'coord': [-74.1453806, 40.5422782]}     Christine'S Restaurant
630           {'coord': [-74.138492, 40.631136]}                Indian Oven
"""

#-----------------------------------------------------------------------------------------------------------------------------#
## AirBnB

# Aide sur les données : https://docs.atlas.mongodb.com/sample-data/sample-airbnb
# Une fois la connexion est créée à la collection dans Python, répondre aux questions suivantes :

#----------------------------------------------------------------------#
# 1. Lister les différents types de logements possibles cf (room_type)

distinct_room_types = airbnb.distinct("room_type")

#print(distinct_room_types)

"""
['Entire home/apt', 'Private room', 'Shared room']
"""

#----------------------------------------------------------------------#
# 2. Lister les différents équipements possibles cf (amenities)
distinct_amenities = airbnb.distinct("amenities")

#print(distinct_amenities)

"""
['', '24-hour check-in', 'Accessible-height bed', 'Accessible-height toilet', 'Air conditioning', 'Air purifier', 'Alfresco shower', 'BBQ grill', 'Baby bath', 'Baby monitor', 'Babysitter recommendations', 'Balcony', 'Bath towel', 'Bathroom essentials', 'Bathtub', 'Bathtub with bath chair', 'Beach chairs', 'Beach essentials', 'Beach view', 'Beachfront', 'Bed linens', 'Bedroom comforts', 'Bicycle', 'Bidet', 'Body soap', 'Boogie boards', 'Breakfast', 'Breakfast bar', 'Breakfast table', 'Building staff', 'Buzzer/wireless intercom', 'Cable TV', 'Carbon monoxide detector', 'Cat(s)', 'Ceiling fan', 'Central air conditioning', 'Changing table', "Chef's kitchen", 'Children’s books and toys', 'Children’s dinnerware', 'Cleaning before checkout', 'Coffee maker', 'Convection oven', 'Cooking basics', 'Crib', 'DVD player', 'Day bed', 'Dining area', 'Disabled parking spot', 'Dishes and silverware', 'Dishwasher', 'Dog(s)', 'Doorman', 'Double oven', 'Dryer', 'EV charger', 'Electric profiling bed', 'Elevator', 'En suite bathroom', 'Espresso machine', 'Essentials', 'Ethernet connection', 'Extra pillows and blankets', 'Family/kid friendly', 'Fax machine', 'Fire extinguisher', 'Fireplace guards', 'Firm mattress', 'First aid kit', 'Fixed grab bars for shower', 'Fixed grab bars for toilet', 'Flat path to front door', 'Formal dining area', 'Free parking on premises', 'Free street parking', 'Full kitchen', 'Game console', 'Garden or backyard', 'Gas oven', 'Ground floor access', 'Gym', 'Hair dryer', 'Handheld shower head', 'Hangers', 'Heated towel rack', 'Heating', 'High chair', 'Home theater', 'Host greets you', 'Hot tub', 'Hot water', 'Hot water kettle', 'Ice Machine', 'Indoor fireplace', 'Internet', 'Iron', 'Ironing Board', 'Kayak', 'Keypad', 'Kitchen', 'Kitchenette', 'Lake access', 'Laptop friendly workspace', 'Lock on bedroom door', 'Lockbox', 'Long term stays allowed', 'Luggage dropoff allowed', 'Memory foam mattress', 'Microwave', 'Mini fridge', 'Mountain view', 'Murphy bed', 'Netflix', 'Other', 'Other pet(s)', 'Outdoor parking', 'Outdoor seating', 'Outlet covers', 'Oven', 'Pack ’n Play/travel crib', 'Paid parking off premises', 'Paid parking on premises', 'Parking', 'Patio or balcony', 'Permit parking', 'Pets allowed', 'Pets live on this property', 'Pillow-top mattress', 'Pocket wifi', 'Pool', 'Pool with pool hoist', 'Private bathroom', 'Private entrance', 'Private hot tub', 'Private living room', 'Private pool', 'Rain shower', 'Refrigerator', 'Roll-in shower', 'Room-darkening shades', 'Safe', 'Safety card', 'Sauna', 'Self check-in', 'Shampoo', 'Shared pool', 'Shower chair', 'Single level home', 'Ski-in/Ski-out', 'Smart TV', 'Smart lock', 'Smoke detector', 'Smoking allowed', 'Snorkeling equipment', 'Sonos sound system', 'Sound system', 'Stair gates', 'Standing valet', 'Step-free access', 'Stove', 'Suitable for events', 'Sun loungers', 'Swimming pool', 'TV', 'Table corner guards', 'Tennis court', 'Terrace', 'Toaster', 'Toilet paper', 'Walk-in shower', 'Warming drawer', 'Washer', 'Washer / Dryer', 'Waterfront', 'Well-lit path to entrance', 'Wheelchair accessible', 'Wide clearance to bed', 'Wide clearance to shower', 'Wide doorway', 'Wide entryway', 'Wide hallway clearance', 'Wifi', 'Window guards', 'toilet', 'translation missing: en.hosting_amenity_49', 'translation missing: en.hosting_amenity_50']
"""

#----------------------------------------------------------------------#
# 3. Donner le nombre de logements

total_posts = airbnb.count_documents({})

#print(total_posts)

"""
5555
"""

#----------------------------------------------------------------------#
# 4. Donner le nombre de logements de type "Entire home/apt"

entire_homeorapt_count = airbnb.count_documents({"room_type": "Entire home/apt"})

#print(entire_homeorapt_count)

"""
3489
"""

#----------------------------------------------------------------------#
# 5. Donner le nombre de logements proposant la TV et le Wifi (cf amenities)
tv_or_wifi_count = airbnb.count_documents({"amenities": {"$all": ["TV", "Wifi"]}})

#print(tv_or_wifi_count)

"""
4140
"""

#----------------------------------------------------------------------#
# 6. Donner le nombre de logements n'ayant eu aucun avis
no_reviews = airbnb.count_documents({"reviews": {"$size": 0}})

#print(no_reviews)

"""
1632
"""

#----------------------------------------------------------------------#
# 7. Lister les informations du logement 10545725 (cf _id)
infos_10545725 = airbnb.find_one({"_id": "10545725"})

#print(infos_10545725)

"""
{'_id': '10545725', 'listing_url': 'https://www.airbnb.com/rooms/10545725', 'name': 'Cozy bedroom Sagrada Familia', 'summary': 'Cozy bedroom next to the church Sagrada Família a great choice to stay in a residential area away from the crowds while still being at a walking distance to main attractions in Barcelona.', 'space': 'Cozy beroom located three minutes from the Sagrada Família in a central zone of Barcelona.  Equiped kitchen.', 'description': "Cozy bedroom next to the church Sagrada Família a great choice to stay in a residential area away from the crowds while still being at a walking distance to main attractions in Barcelona. Cozy beroom located three minutes from the Sagrada Família in a central zone of Barcelona.  Equiped kitchen. I'll be glad to give you some tips according to your taste. Well located on a calm residential area a few blocks away from the city center. Metro on the corner 5 stops away from the Ramblas, Paseo de Gracia and Catalunya square. Restaurants, bars and supermarket around the area.", 'neighborhood_overview': 'Well located on a calm residential area a few blocks away from the city center.', 'notes': '', 'transit': 'Metro on the corner 5 stops away from the Ramblas, Paseo de Gracia and Catalunya square. Restaurants, bars and supermarket around the area.', 'access': '', 'interaction': "I'll be glad to give you some tips according to your taste.", 'house_rules': '', 'property_type': 'Apartment', 'room_type': 'Private room', 'bed_type': 'Real Bed', 'minimum_nights': '2', 'maximum_nights': '1125', 'cancellation_policy': 'flexible', 'last_scraped': datetime.datetime(2019, 3, 8, 5, 0), 'calendar_last_scraped': datetime.datetime(2019, 3, 8, 5, 0), 'first_review': datetime.datetime(2016, 2, 14, 5, 0), 'last_review': datetime.datetime(2016, 2, 14, 5, 0), 'accommodates': 2, 'bedrooms': 1, 'beds': 2, 'number_of_reviews': 1, 'bathrooms': Decimal128('1.0'), 'amenities': ['TV', 'Internet', 'Wifi', 'Kitchen', 'Pets allowed', 'Buzzer/wireless intercom', 'Family/kid friendly', 'Washer', 'Essentials', 'Laptop friendly workspace'], 'price': Decimal128('20.00'), 'weekly_price': Decimal128('280.00'), 'monthly_price': Decimal128('1080.00'), 'cleaning_fee': Decimal128('20.00'), 'extra_people': Decimal128('0.00'), 'guests_included': Decimal128('1'), 'images': {'thumbnail_url': '', 'medium_url': '', 'picture_url': 'https://a0.muscache.com/im/pictures/953b3c09-adb5-4d1c-a403-b3e61c8fa766.jpg?aki_policy=large', 'xl_picture_url': ''}, 'host': {'host_id': '1929411', 'host_url': 'https://www.airbnb.com/users/show/1929411', 'host_name': 'Rapha', 'host_location': 'Barcelona, Catalonia, Spain', 'host_about': "Hi, I'm from Brazil, but live in Barcelona.\r\nI'm an sportsman, who loves music and is organized.\r\nReally looking foward to a nice deal.\r\nCya,\r\nRapha", 'host_thumbnail_url': 'https://a0.muscache.com/im/users/1929411/profile_pic/1332942535/original.jpg?aki_policy=profile_small', 'host_picture_url': 'https://a0.muscache.com/im/users/1929411/profile_pic/1332942535/original.jpg?aki_policy=profile_x_medium', 'host_neighbourhood': 'el Fort Pienc', 'host_is_superhost': False, 'host_has_profile_pic': True, 'host_identity_verified': True, 'host_listings_count': 1, 'host_total_listings_count': 1, 'host_verifications': ['email', 'phone', 'facebook', 'reviews', 'jumio', 'government_id']}, 'address': {'street': 'Barcelona, Catalunya, Spain', 'suburb': 'Eixample', 'government_area': 'el Fort Pienc', 'market': 'Barcelona', 'country': 'Spain', 'country_code': 'ES', 'location': {'type': 'Point', 'coordinates': [2.17963, 41.40087], 'is_location_exact': True}}, 'availability': {'availability_30': 0, 'availability_60': 0, 'availability_90': 0, 'availability_365': 0}, 'review_scores': {'review_scores_accuracy': 10, 'review_scores_cleanliness': 10, 'review_scores_checkin': 10, 'review_scores_communication': 10, 'review_scores_location': 10, 'review_scores_value': 10, 'review_scores_rating': 100}, 'reviews': [{'_id': '62460002', 'date': datetime.datetime(2016, 2, 14, 5, 0), 'listing_id': '10545725', 'reviewer_id': '50195256', 'reviewer_name': 'Miffy', 'comments': 'Rapha place is so tidy and the dog there is so friendly. I and my friend had a really good time there. Rapha is such a friendly host and he will always give us some suggestions of restaurants or place to travel. The location of the flat is easy to find and it is just next to the metro station and that is very convenient. The hot tourist spot Sagrada Familia is just next to the flat. We can go to the city centre just by walking. Even though at night, we still feel safe to walk to the apartment from the metro.I strongly recommend people to stay there during staying in Barcelona. '}]}
"""

#----------------------------------------------------------------------#
# 8. Lister le nom, la rue et le pays des logements dont le prix est supérieur à 10000

fkcin_expensive_places = pandas.DataFrame(airbnb.find({"price": {"$gt": 10000}}, {"name": 1, "address.street": 1, "address.country": 1}))

#print(fkcin_expensive_places)

"""
 _id                                               name                                            address
0  13997910      Apartamento de luxo em Copacabana - 4 quartos  {'street': 'Rio de Janeiro, Rio de Janeiro, Br...
1  14644562                                          良德街3号温馨住宅  {'street': 'HK, Hong Kong', 'country': 'Hong K...
2  20275354  İstanbul un kalbi sisli. Center of istanbul sisli  {'street': 'Şişli, İstanbul, Turkey', 'country...
3  22200454                                      `LM 三個睡房的整间公寓  {'street': 'Hong Kong, Kowloon, Hong Kong', 'c...
4  27593455                       HS1-2人大床房+丰泽､苏宁､百脑汇+女人街+美食中心  {'street': 'Hong Kong, Kowloon, Hong Kong', 'c...
5  30327756               5 PEOPLE ROOM ( 1 TRIP and 1 DOUBLE)  {'street': 'Hong Kong, Kowloon, Hong Kong', 'c...
"""

#----------------------------------------------------------------------#
# 9. Donner le nombre de logements par type
list_by_type = pandas.DataFrame(
    airbnb.aggregate(
            [
        {"$group": {"_id": "$room_type", "count": {"$sum": 1}}}
        ]
    )
)

#print(list_by_type)
"""
               _id  count
0     Private room   1983
1      Shared room     83
2  Entire home/apt   3489
"""

#----------------------------------------------------------------------#
# 10. Donner le nombre de logements par pays
list_by_country = pandas.DataFrame(
    airbnb.aggregate(
            [
        {"$group": {"_id": "$address.country", "count": {"$sum": 1}}}
        ]
    )
)

#print (list_by_country)

"""
             _id  count
0          Spain    633
1      Australia    610
2         Brazil    606
3       Portugal    555
4         Turkey    661
5         Canada    649
6          China     19
7      Hong Kong    600
8  United States   1222
"""

#----------------------------------------------------------------------#
# 11. On veut représenter graphiquement la distribution des prix, il nous faut donc récupérer uniquement les tarifs
#     - Un tarif apparaissant plusieurs fois dans la base doit être présent plusieurs fois dans cette liste



def NuageDePointsDesPrix():
    prices = [listing['price'] for listing in airbnb.find({}, {'price': 1})]
    prices_converted = [float(price.to_decimal()) if isinstance(price, Decimal128) else float(price) for price in prices if price is not None]
    cleaned_prices = [p for p in prices_converted if not np.isnan(p)]
    filtered_prices = [p for p in cleaned_prices if p <= 13000]
    sns.stripplot(x=filtered_prices, jitter=0.35, size=3, alpha=0.5)
    plt.title('Distribution des Tarifs Airbnb')
    plt.xlabel('Tarif')
    plt.ylabel('Fréquence (superposition des points)')
    plt.xlim(0, 13000)
    plt.show()

# NuageDePointsDesPrix()

#----------------------------------------------------------------------#
# 12. Calculer pour chaque type de logements (room_type) le prix (price)
    
def PrixMoyenParTypeDeLogement():
    aggregation_result = airbnb.aggregate([
        {"$group": {
            "_id": "$room_type", 
            "average_price": {"$avg": "$price"}
        }}
    ])
    
    average_price_by_room_type = pandas.DataFrame([
        {"_id": room['_id'], "average_price": float(room['average_price'].to_decimal())}
        for room in aggregation_result
    ])

    sns.set_style("whitegrid")
    
    sns.barplot(data=average_price_by_room_type, x='_id', y='average_price', palette="mako")
    
    plt.title('Prix moyen par type de logement AirBnb')
    plt.xlabel('Type de logement')
    plt.ylabel('Prix moyen')
    
    plt.show()

PrixMoyenParTypeDeLogement()

#----------------------------------------------------------------------#
# 13. On veut représenter la distribution du nombre d'avis. Il faut donc calculer pour chaque logement le nombre d'avis qu'il a eu (cf reviews)

def DistributionduNombredAvis():
    review_counts = pandas.DataFrame(airbnb.aggregate([
        {"$project": {"_id": 1, "number_of_reviews": {"$size": "$reviews"}}}
    ]))
    sns.set_style("whitegrid")
    sns.histplot(review_counts['number_of_reviews'], kde=False, bins=30, color="skyblue", edgecolor='black')
    plt.title('Représentation de la distribution du nombre d\'avis')
    plt.xlabel('Nombre d\'avis')
    plt.ylabel('Fréquence')
    plt.show()

DistributionduNombredAvis()

#----------------------------------------------------------------------#
# 14. Compter le nombre de logement pour chaque équipement possible
amenity_listing = pandas.DataFrame(airbnb.aggregate([
    {"$unwind": "$amenities"},
    {"$group": {"_id": "$amenities", "count": {"$sum": 1}}}
]))

#print(amenity_listing)

"""
 _id  count
0                      Hangers   4226
1        Children’s dinnerware    137
2          Table corner guards     24
3                      Day bed      2
4            Fire extinguisher   2207
..                         ...    ...
181               Coffee maker   1450
182      Dishes and silverware   1717
183  Laptop friendly workspace   3442
184      Wide clearance to bed    275
185  Children’s books and toys    283
"""
#----------------------------------------------------------------------#
# 15. On souhaite connaître les 10 utilisateurs ayant fait le plus de commentaires
top_reviewers = pandas.DataFrame(airbnb.aggregate([
    {"$unwind": "$reviews"},
    {"$group": {"_id": "$reviews.reviewer_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]))

#print(top_reviewers)

"""
         _id  count
0   20775242     24
1   67084875     13
2    2961855     10
3  162027327      9
4   20991911      9
5   60816198      8
6    1705870      8
7   55241576      8
8   12679057      8
9   69140895      8
"""
#----------------------------------------------------------------------#
