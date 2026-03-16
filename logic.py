from haversine import haversine_km

def validate_intel(data):
    valid = ['entity_id', 'reported_lat', 'reported_lon', 'timestamp']

    for field in valid:
        if field not in valid:
            return False, f"Missing field: {field}"
    return True, None

def calculate_dist(prew_lat, prew_lon, new_lat, new_lon):
    run_calculate = haversine_km(prew_lat, prew_lon, new_lat, new_lon)
    return run_calculate

