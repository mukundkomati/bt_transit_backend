import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from sqlalchemy.orm import Session
from database import engine, SessionLocal
from models import Base, Route, Stop, Shape, Trip, StopTime, Calendar
from gtfs_realtime_pb2 import FeedMessage  # For parsing GTFS-realtime data
import requests
import json
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from envConfig import (
    GTFS_REAL_TIME_POSITION_UPDATES_URL,
    GTFS_REAL_TIME_TRIP_UPDATES_URL,
    GTFS_REAL_TIME_ALERTS_URL,
)
import traceback
from datetime import date

# Set up logging for debugging and tracking application behavior
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI()

# Configure CORS (Cross-Origin Resource Sharing) to allow all origins
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables from models
Base.metadata.create_all(bind=engine)

# Dependency for managing database sessions
# Ensures each request uses a clean session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Track active WebSocket clients
connected_clients = set()

# Root endpoint to verify server status
@app.get("/")
async def root():
    return {"message": "Hello World"}

# Endpoint to retrieve all routes
@app.get("/routes")
def get_routes(db: Session = Depends(get_db)):
    """
    Fetch all available routes from the database.
    """
    try:
        routes = db.query(Route).all()
        return routes
    except Exception as e:
        logger.error(f"Error fetching routes: {e}")
        return {"error": "Failed to retrieve routes"}

# Endpoint to retrieve a specific route by its ID
@app.get("/routes/{route_id}")
def get_route(route_id: str, db: Session = Depends(get_db)):
    """
    Fetch a specific route by its unique route_id.
    """
    try:
        route = db.query(Route).filter(Route.route_id == route_id).first()
        if route is None:
            raise HTTPException(status_code=404, detail="Route not found")
        return route
    except Exception as e:
        logger.error(f"Error fetching route {route_id}: {e}")
        return {"error": "Failed to retrieve route"}

# Endpoint to retrieve all stops
@app.get("/stops")
def get_stops(db: Session = Depends(get_db)):
    """
    Fetch all stops available in the database.
    """
    try:
        stops = db.query(Stop).all()
        return stops
    except Exception as e:
        logger.error(f"Error fetching stops: {e}")
        return {"error": "Failed to retrieve stops"}

# Endpoint to retrieve details for all routes, including shapes and stops
# Reference: Example of handling database relationships and nested queries
# URL: https://docs.sqlalchemy.org/en/14/orm/tutorial.html#working-with-related-objects
@app.get("/all-routes/details")
def get_all_routes_details(db: Session = Depends(get_db)):
    """
    Fetch detailed information for all routes, including shapes and stops.
    """
    try:
        routes = db.query(Route).all()
        if not routes:
            raise HTTPException(status_code=404, detail="No routes found")

        routes_details = []  # Store details for all routes

        for route in routes:
            # Retrieve trips associated with the route
            trips = db.query(Trip).filter(Trip.route_id == route.route_id).all()
            if not trips:
                continue

            # Collect shape details for each trip
            unique_shape_ids = list({trip.shape_id for trip in trips if trip.shape_id})
            all_shapes = [
                {
                    "latitude": shape.shape_pt_lat,
                    "longitude": shape.shape_pt_lon,
                    "sequence": shape.shape_pt_sequence,
                    "shape_id": shape.shape_id,
                }
                for shape_id in unique_shape_ids
                for shape in db.query(Shape)
                .filter(Shape.shape_id == shape_id)
                .order_by(Shape.shape_pt_sequence)
                .all()
            ]

            # Collect stop details for each trip
            stop_ids = {
                stop_time.stop_id
                for trip in trips
                for stop_time in db.query(StopTime).filter(
                    StopTime.trip_id == trip.trip_id
                ).all()
            }
            stops = db.query(Stop).filter(Stop.stop_id.in_(stop_ids)).all()
            stop_coordinates = [
                {
                    "latitude": stop.stop_lat,
                    "longitude": stop.stop_lon,
                    "stop_name": stop.stop_name,
                }
                for stop in stops
            ]

            # Combine route, shape, and stop data
            routes_details.append(
                {
                    "route": route,
                    "shape": all_shapes,
                    "stops": stop_coordinates,
                }
            )

        if not routes_details:
            raise HTTPException(status_code=404, detail="No route details found")

        return {"routes": routes_details}
    except Exception as e:
        logger.error(f"Error fetching all route details: {e}")
        return {"error": "Failed to retrieve route details"}

# Function to load GTFS-realtime protocol buffer data from a URL
# Reference: Parsing GTFS-realtime data using Python Protobuf
# URL: https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md
async def load_pb_from_url(url):
    """
    Load GTFS-realtime data from the specified URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        feed = FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except Exception as e:
        logger.error(f"Error loading data from URL {url}: {e}")
        logger.debug(traceback.format_exc())
        return None

# Function to fetch and process bus positions
# Reference: Parsing vehicle position updates in GTFS-realtime
# URL: https://github.com/MobilityData/gtfs-realtime-bindings/blob/master/python/README.md
async def fetch_bus_positions(db: Session):
    """
    Fetch real-time bus positions from GTFS-realtime feed and associate them with routes.
    """
    try:
        url = GTFS_REAL_TIME_POSITION_UPDATES_URL
        feed = await load_pb_from_url(url)
        positions = []

        if not feed:
            return {"positions": []}

        for entity in feed.entity:
            if entity.HasField("vehicle"):
                vehicle_id = entity.vehicle.vehicle.id
                trip_id = entity.vehicle.trip.trip_id
                latitude = entity.vehicle.position.latitude
                longitude = entity.vehicle.position.longitude
                bearing = entity.vehicle.position.bearing

                # Fetch route details for the trip
                trip = db.query(Trip).filter(Trip.trip_id == trip_id).first()
                if trip:
                    route = db.query(Route).filter(Route.route_id == trip.route_id).first()
                    if route:
                        positions.append(
                            {
                                "vehicle_id": vehicle_id,
                                "latitude": latitude,
                                "longitude": longitude,
                                "bearing": bearing,
                                "route_id": route.route_id,
                                "route_short_name": route.route_short_name,
                                "route_color": route.route_color,
                            }
                        )
        return {"positions": positions}
    except Exception as e:
        logger.error(f"Error fetching real-time positions: {e}")
        return {"positions": []}

# WebSocket endpoint to stream real-time bus positions
# Reference: FastAPI WebSocket usage
# URL: https://fastapi.tiangolo.com/advanced/websockets/
@app.websocket("/ws/bus-positions")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    """
    Provide real-time bus positions through a WebSocket connection.
    """
    await websocket.accept()
    connected_clients.add(websocket)
    logger.info("Client connected")

    previous_positions = {}  # Keyed by vehicle_id

    def positions_are_different(pos1, pos2):
        lat1, lon1 = pos1
        lat2, lon2 = pos2
        # Compare positions rounded to 6 decimal places to avoid minor floating-point differences
        return round(lat1, 6) != round(lat2, 6) or round(lon1, 6) != round(lon2, 6)

    try:
        while True:
            # Fetch the latest positions and send to client
            bus_positions = await fetch_bus_positions(db)
            if bus_positions:
                positions_changed = False
                current_positions = {}

                for bus in bus_positions["positions"]:
                    vehicle_id = bus["vehicle_id"]
                    current_position = (bus["latitude"], bus["longitude"])
                    previous_position = previous_positions.get(vehicle_id)

                    # Check if positions are different
                    if previous_position is None or positions_are_different(
                        previous_position, current_position
                    ):
                        positions_changed = True
                        # Update current_positions
                        current_positions[vehicle_id] = current_position
                    else:
                        # No change, keep previous position
                        current_positions[vehicle_id] = previous_position

                # Send to front end only if there are any changes
                if positions_changed:
                    message = json.dumps(bus_positions)
                    await websocket.send_text(message)

                # Update previous_positions
                previous_positions = current_positions

            await asyncio.sleep(2)  # Send updates every 2 seconds
    except WebSocketDisconnect:
        logger.info("Client disconnected")
        connected_clients.remove(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        connected_clients.remove(websocket)

# Shutdown handler to close WebSocket connections gracefully
@app.on_event("shutdown")
async def on_shutdown():
    for client in connected_clients:
        await client.close()


@app.get("/real-time-trips")
def get_real_time_trips():
    try:
        url = GTFS_REAL_TIME_TRIP_UPDATES_URL
        feed = load_pb_from_url(url)
        trips = [
            {
                "trip_id": entity.trip_update.trip.trip_id,
                "route_id": entity.trip_update.trip.route_id,
                "start_time": entity.trip_update.trip.start_time,
                "start_date": entity.trip_update.trip.start_date,
                "stop_time_updates": [
                    {
                        "stop_id": update.stop_id,
                        "arrival": update.arrival.time if update.HasField("arrival") else None,
                        "departure": update.departure.time if update.HasField("departure") else None,
                    }
                    for update in entity.trip_update.stop_time_update
                ],
            }
            for entity in feed.entity
            if entity.HasField("trip_update")
        ]
        return {"trips": trips}
    except Exception as e:
        logger.error(f"Error fetching real-time trips: {e}")
        logger.debug(traceback.format_exc())
        return {"error": "Failed to retrieve real-time trips"}


# Real-time Alerts Endpoint
@app.get("/real-time-alerts")
def get_real_time_alerts():
    try:
        url = GTFS_REAL_TIME_ALERTS_URL
        feed = load_pb_from_url(url)
        alerts = [
            {
                "alert_id": entity.id,
                "cause": entity.alert.cause,
                "effect": entity.alert.effect,
                "header_text": entity.alert.header_text.translation[0].text
                if entity.alert.header_text.translation
                else None,
                "description_text": entity.alert.description_text.translation[0].text
                if entity.alert.description_text.translation
                else None,
                "informed_entity": [
                    {
                        "agency_id": informed.agency_id,
                        "route_id": informed.route_id,
                        "stop_id": informed.stop_id,
                    }
                    for informed in entity.alert.informed_entity
                ],
            }
            for entity in feed.entity
            if entity.HasField("alert")
        ]
        return {"alerts": alerts}
    except Exception as e:
        logger.error(f"Error fetching real-time alerts: {e}")
        logger.debug(traceback.format_exc())
        return {"error": "Failed to retrieve real-time alerts"}


@app.get("/routes/{route_id}/schedule")
def get_route_schedule(route_id: str, db: Session = Depends(get_db)):
    try:
        service_date = date.today()

        # Get weekday name in lowercase (e.g., 'monday', 'tuesday')
        weekday = service_date.strftime("%A").lower()

        # Fetch services active on the current date
        active_services = (
            db.query(Calendar)
            .filter(
                getattr(Calendar, weekday) == True,
                Calendar.start_date <= service_date,
                Calendar.end_date >= service_date,
            )
            .all()
        )

        if not active_services:
            return {"schedule": [], "message": "No active services today."}

        service_ids = [service.service_id for service in active_services]

        # Fetch trips for the route with active service IDs
        trips = (
            db.query(Trip)
            .filter(Trip.route_id == route_id, Trip.service_id.in_(service_ids))
            .all()
        )

        if not trips:
            return {"schedule": [], "message": "No trips found for this route today."}

        trip_ids = [trip.trip_id for trip in trips]

        # Fetch stop times for these trips
        stop_times = (
            db.query(StopTime)
            .filter(StopTime.trip_id.in_(trip_ids))
            .order_by(StopTime.trip_id, StopTime.stop_sequence)
            .all()
        )

        # Fetch stops information
        stop_ids = list(set([st.stop_id for st in stop_times]))
        stops = db.query(Stop).filter(Stop.stop_id.in_(stop_ids)).all()
        stop_map = {stop.stop_id: stop for stop in stops}

        # Organize schedule data
        schedule = []
        for trip in trips:
            trip_stop_times = [st for st in stop_times if st.trip_id == trip.trip_id]
            trip_schedule = {
                "trip_id": trip.trip_id,
                "trip_headsign": trip.trip_headsign,
                "direction_id": trip.direction_id,
                "stop_times": [],
            }
            for st in trip_stop_times:
                stop = stop_map.get(st.stop_id)
                if not stop:
                    continue
                trip_schedule["stop_times"].append(
                    {
                        "stop_id": st.stop_id,
                        "stop_name": stop.stop_name,
                        "arrival_time": st.arrival_time,
                        "departure_time": st.departure_time,
                        "stop_sequence": st.stop_sequence,  # Include stop_sequence
                    }
                )
            schedule.append(trip_schedule)

        return {"schedule": schedule}

    except Exception as e:
        print(f"Error fetching schedule for route {route_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to retrieve schedule")
