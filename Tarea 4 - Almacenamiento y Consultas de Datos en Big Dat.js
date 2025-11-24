[
    {
        '$group': {
            '_id': '$Brand', 
            'avgMileage': {
                '$avg': '$Mileage'
            }
        }
    }, {
        '$group': {
            '_id': '$Brand', 
            'totalMileage': {
                '$sum': '$Mileage'
            }
        }
    }, {
        '$count': 'total_cars'
    }
]
