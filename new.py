import streamlit as st
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from geopy.geocoders import Nominatim
from datetime import datetime
import uuid
import requests

# Connect to Astra DB


from config import astra_client_id, astra_client_secret, astra_database_id, astra_app_name

# # Function to connect to Cassandra database
def connect_db():
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-cassandra-db.zip'  # Path to your secure connect bundle
    }
    cluster = Cluster(cloud=cloud_config, auth_provider=PlainTextAuthProvider(astra_client_id, astra_client_secret))
    session = cluster.connect()
    session.set_keyspace("system1")
    return session



# def connect_db():
#     try:
#         cloud_config = {'secure_connect_bundle': 'secure-connect-cassandra-db.zip'}
#         auth_provider = PlainTextAuthProvider('hbhfmkOZekcgsQIWbkrYPwvq', 'aZ4UZiDtNfA_tHdZ1eMem2_aw3LUUo71oADWlhqTTA3vi,wu8wZDjI0aQ0.Nj0emJh-8m+6mLu37GW8R3s8y_KrvUhUuev,2PlZ61F2fGU2IGiM.-obFQyoQhx+LT78d')
#         cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
#         session = cluster.connect()
#         print("Connection successful!")
#     except Exception as e:
#         print(f"Error: {e}")
    
#     return session
# Initialize Cassandra session
session = connect_db()

# Create tables if not exist
def create_tables():
    session.execute("""
        CREATE TABLE IF NOT EXISTS profile (
            id UUID PRIMARY KEY,
            username TEXT,
            password TEXT,
            location TEXT
        )
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS admin_events (
            id UUID PRIMARY KEY,
            event_name TEXT,
            event_time TIMESTAMP,
            event_location TEXT,
            event_type TEXT
        )
    """)


# Function to get current location
def get_current_location():
    try:
        response = requests.get('http://ip-api.com/json/')  # Free geolocation API
        data = response.json()
        if response.status_code == 200:
            return f"{data['city']}"
        else:
            return "Location Unavailable"
    except Exception as e:
        st.error("Error fetching location. Ensure you have internet access.")
        return "Location Error"

# Function to create a user-specific table

def create_user_table(username):
    # Create a table for user info (already provided in your code)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {username}_user (
            id UUID PRIMARY KEY,
            login_time TIMESTAMP,
            curr_location TEXT
        )
    """)
    
    # Create a table for user preferences
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {username}_preferences (
            id UUID PRIMARY KEY,
            event_type TEXT,
            event_time TIMESTAMP,
            event_location TEXT
        )
    """)

    # Create the bookings table
    session.execute(f"""
    CREATE TABLE IF NOT EXISTS {username}_bookings (
        id UUID PRIMARY KEY,
        event_name TEXT,
        event_date TIMESTAMP,
        event_location TEXT,
        event_type TEXT,
        status TEXT  -- status could be 'booked', 'cancelled', etc.
    )
""")




# Sign up functionality
def sign_up():
    st.title("Sign Up")
    username = st.text_input("Enter Username")
    password = st.text_input("Enter Password", type="password")
    if st.button("Sign Up"):
        if username and password:
            location = get_current_location()
            # Add user to the profile table
            session.execute("""
                INSERT INTO profile (id, username, password, location)
                VALUES (%s, %s, %s, %s)
            """, (uuid.uuid4(), username, password, location))
            # Create user-specific table
            create_user_table(username)
            st.success("Sign Up Successful! Please log in.")
        else:
            st.error("Username and Password are required.")

# Login functionality
def login():
    st.title("Login")
    username = st.text_input("Enter Username")
    password = st.text_input("Enter Password", type="password")
    if st.button("Login"):
        # Modified query with ALLOW FILTERING
        result = session.execute("""
            SELECT * FROM profile WHERE username=%s AND password=%s ALLOW FILTERING
        """, (username, password))
        user = result.one()
        if user:
            st.session_state["logged_in"] = True
            st.session_state["username"] = username
            st.success(f"Welcome, {username}!")
            # Record login details in the user's table
            curr_location = get_current_location()
            session.execute(f"""
                INSERT INTO {username}_user (id, login_time, curr_location)
                VALUES (%s, %s, %s)
            """, (uuid.uuid4(), datetime.now(), curr_location))
            st.rerun()
        else:
            st.error("Invalid username or password.")

# Admin functionality
def admin():
    st.title("Admin Page")
    event_name = st.text_input("Event Name")
    event_date = st.date_input("Event Date")
    event_time1 = st.time_input("Event Time")

# Combine the date and time into a single datetime object
    event_time = datetime.combine(event_date, event_time1)
    event_location = st.text_input("Event Location")
    event_type = st.text_input("Event Type")
    if st.button("Add Event"):
        session.execute("""
            INSERT INTO admin_events (id, event_name, event_time, event_location, event_type)
            VALUES (%s, %s, %s, %s, %s)
        """, (uuid.uuid4(), event_name, event_time, event_location, event_type))
        st.success("Event added successfully!")

# Tabs for events and chatbot
from groq import Groq
import os

client = Groq(api_key=st.secrets["GROQ_API_KEY"])

def get_event_suggestions(user_name, query):
    # Connect to the Cassandra database (ensure the connection is valid)
    # cluster = Cluster()
    # session = cluster.connect()

    # Set the keyspace for your database (ensure this is properly set up)
    # session.set_keyspace('your_keyspace')

    # Fetch the user's table (assuming it's named {user_name}_user)
    user_query = f"SELECT curr_location FROM {user_name}_user"
    user_data = session.execute(user_query)
    user_location = None

    # Assuming there is only one record for the user (you can handle multiple if necessary)
    for row in user_data:
        user_location = row.curr_location  # Fetch user location

    print(f"User Location: {user_location}")  # Debugging line

    # Fetch all events from the admin_events table
    event_query = "SELECT event_name, event_type, event_location, event_time FROM admin_events"
    events = session.execute(event_query)
    events_list = [event for event in events]  # Convert ResultSet to list
    print(f"Fetched Events: {events_list}")  # Debugging line

    # List to store matching events
    matching_events = []

    # First, compare the event_location of the user with the admin_events
    if user_location:
        # Loop through the admin events to prioritize location matching
        for event in events_list:
            if event.event_location == user_location:
                matching_events.append(event)

    # If no events matched based on location, then use preferences matching logic
    if not matching_events:
        # Fetch user preferences from the preferences table
        preference_query = f"SELECT event_type, event_location, event_time FROM {user_name}_preferences"
        preferences = session.execute(preference_query)
        preferences_list = [pref for pref in preferences]  # Convert ResultSet to list
        print(f"User Preferences: {preferences_list}")  # Debugging line

        # Loop through the admin events and check for matches with preferences
        for event in events_list:
            for pref in preferences_list:
                if (pref.event_type == event.event_type or
                    pref.event_location == event.event_location or
                    pref.event_time == event.event_time):
                    matching_events.append(event)

    # If still no match, return all events with a message indicating no match found
    if not matching_events:
        matching_events = events_list
        return "No preferences matched, here are all events available:\n" + format_events(events_list)

    # Convert event details to a readable string format for matching events
    event_list = format_events(matching_events)
    return event_list




def format_events(events):
    """
    Helper function to format events in a readable string format.
    """
    if not events:  # Check if events are empty
        return "No events available."
    
    return "\n".join([f"Event Name: {event.event_name}, Type: {event.event_type}, Location: {event.event_location}, Time: {event.event_time} \n" for event in events])


# Function to handle user queries and get responses from Groq
def handle_user_query(query, user_name):
    if "suggest me an event" in query.lower() or "event" in query.lower():
        event_suggestions = get_event_suggestions(user_name, query)
        return event_suggestions
    else:
        messages = [
            {"role": "system", "content": "You are a chatbot that helps users suggest events based on their preferences. For non-event-related questions, you answer based on your training."},
            {"role": "user", "content": query}
        ]
        
        try:
            chat_completion = client.chat.completions.create(
                messages=messages,
                model="llama3-70b-8192"
            )
            if chat_completion.choices:
                return chat_completion.choices[0].message.content
            else:
                return "Sorry, I couldn't process your request. Please try again."
        except Exception as e:
            return f"Error: {str(e)}"




def tabs(username):
    tab1, tab2 = st.tabs(["Events", "Chatbot"])
    
    with tab1:
        st.write("Events Page")
        
        # Fetch events from the database
        rows = session.execute("SELECT event_name, event_time, event_location, event_type FROM admin_events")
        
        # Create columns for displaying events in a grid
        num_columns = 4  # Number of columns you want
        columns = st.columns(num_columns)  # Create the columns

        # Loop through the events and display them in the columns
        for i, row in enumerate(rows):
            # Determine which column to use
            col = columns[i % num_columns]
            
            # Dummy image path (replace with actual event image URLs or a correct local path)
            image_path = "cricket.jpeg"
            
            # Display event card with image and details
            with col:
                st.markdown(
                    f"""
                    <div style="background-color: white; padding: 15px; margin-bottom: 15px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); text-align: left;">
                        <div style="color: blue; font-size: 16px; margin-top: 5px; text-align: left;"><strong>Event Name:</strong> {row.event_name}</div>
                        <div style="color: blue; font-size: 16px; margin-top: 5px; text-align: left;"><strong>Event Time:</strong> {row.event_time}</div>
                        <div style="color: blue; font-size: 16px; margin-top: 5px; text-align: left;"><strong>Event Location:</strong> {row.event_location}</div>
                        <div style="color: blue; font-size: 16px; margin-top: 5px; text-align: left;"><strong>Event Type:</strong> {row.event_type}</div>
                        
                    </div>
                    """, 
                    unsafe_allow_html=True
                )
                
                # When the "Book" button is clicked, store the event details in the bookings table
                # When the "Book" button is clicked, store the event details in the bookings table
                if st.button(f"Book {row.event_name}", key=f"book_{row.event_name}"):
                    try:
                        # Insert the booking details into the bookings table using parameterized queries
                        session.execute("""
                            INSERT INTO {0}_bookings (id, event_name, event_date, event_location, event_type, status)
                            VALUES (uuid(), %s, %s, %s, %s, %s)
                        """.format(username), 
                        (row.event_name, row.event_time, row.event_location, row.event_type, 'booked'))

                        # Insert the event details into the preferences table
                        session.execute("""
                            INSERT INTO {0}_preferences (id, event_type, event_time, event_location)
                            VALUES (uuid(), %s, %s, %s)
                        """.format(username), 
                        (row.event_type, row.event_time, row.event_location))

                        # Show a confirmation message
                        st.success(f"Successfully booked {row.event_name}!")
                    except Exception as e:
                        st.error(f"Error booking event: {str(e)}")
                        print(f"Error booking event: {str(e)}")





    with tab2:
        st.write("Chatbot for Event Suggestions")
    
        # Get the user's name for personalized queries (can be added via a login system)
        user_name = st.text_input("Enter your username")
        
        # First button to show the query input
        if st.button("yeah"):
            user_query = st.text_input("Ask something about events or general queries:")
            
            if user_query:  # Check if the user query is not empty
                # Second button to submit the query
                if st.button("SUBMIT"):
                    response = handle_user_query(user_query, user_name)
                    st.write(response)
            else:
                st.warning("Please enter a query to get a response.")








# Sidebar navigation
def main():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    st.sidebar.title("Navigation")
    choice = st.sidebar.radio("Choose an option", ["Sign Up", "Login", "Admin"])

    if choice == "Sign Up":
        sign_up()
    elif choice == "Login":
        if st.session_state["logged_in"]:
            tabs(st.session_state["username"])
        else:
            login()
    elif choice == "Admin":
        admin()

if __name__ == "__main__":
    main()
