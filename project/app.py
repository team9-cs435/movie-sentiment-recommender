import streamlit as st
import pandas as pd

# Must be the absolute first Streamlit command
st.set_page_config(
    page_title="CineSpark Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS matching the clean dashboard UI aesthetic
st.markdown("""
    <style>
    /* Global Background & Font */
    .stApp {
        background-color: #eef1f6;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }

    /* Main Content Area */
    .main .block-container {
        padding: 2rem 3rem;
    }

    /* Card Containers */
    .dashboard-card {
        background-color: #ffffff;
        padding: 24px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03);
        margin-bottom: 20px;
    }

    /* Primary Lime Green Buttons */
    .stButton>button {
        background-color: #8cc63f !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        font-weight: 600 !important;
        padding: 0.6rem 1rem !important;
        box-shadow: 0 4px 12px rgba(140, 198, 63, 0.3) !important;
        transition: all 0.2s ease-in-out;
    }
    .stButton>button:hover {
        background-color: #7bb335 !important;
        box-shadow: 0 6px 16px rgba(140, 198, 63, 0.4) !important;
    }

    /* Secondary / Delete Buttons */
    button[kind="secondary"] {
        background-color: #ffffff !important;
        color: #e74c3c !important;
        border: 1px solid #e74c3c !important;
        box-shadow: none !important;
    }
    button[kind="secondary"]:hover {
        background-color: #fdf2f2 !important;
    }

    /* Input Fields */
    .stTextInput>div>div>input, .stSelectbox>div>div>div {
        background-color: #f8f9fc !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 10px !important;
        color: #2d3748 !important;
    }

    /* Headers */
    h1, h2, h3 {
        color: #1a202c;
        font-weight: 700;
    }

    p, span, label {
        color: #4a5568;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize Session State for Mock Database
if "users" not in st.session_state:
    st.session_state.users = {
        "user_demo_123": {"name": "Adam Stonebridge", "language": "English", "watched": {"Timmy Time: Season 3": 5.0}}
    }
if "current_user" not in st.session_state:
    st.session_state.current_user = None

if "navigation" not in st.session_state:
    st.session_state.navigation = "home"

# Mock Catalog data
if "catalog" not in st.session_state:
    st.session_state.catalog = [
        {"id": "B00CE10AP8", "title": "Timmy Time: Season 3", "category": "Comedy",
         "url": "https://images-na.ssl-images-amazon.com/images/S/pv-target-images/14793d49cfb72847f74c85cf009d1c2966bdb2c3a3c06911a82f83372bcd07be._RI_SX300_FMwebp_.jpg"},
        {"id": "B09WPZSR8N", "title": "PBS KIDS: Secret Superheroes!", "category": "Kids",
         "url": "https://m.media-amazon.com/images/I/51hwsu9AuGL.jpg"},
        {"id": "B00AHN851G", "title": "How to Make Animatronic Characters", "category": "Documentary",
         "url": "https://m.media-amazon.com/images/I/51xAtwVr3ML.jpg"},
        {"id": "B07NJ288Q1", "title": "Chippendales: Off the Cuff", "category": "Documentary",
         "url": "https://images-na.ssl-images-amazon.com/images/S/pv-target-images/68a62722ed53eb2890648cb790ba7e991b042e6d57875146a8e7d6a7780d6aec._RI_SX300_FMwebp_.jpg"},
        {"id": "B01N41M6VP", "title": "Game of Thrones Homemade Intro", "category": "Comedy",
         "url": "https://images-media-amazon.com/images/G/01/digital/video/web/Default_Background_Art_LTR._SX300_FMwebp_.jpg"}
    ]

if "hidden_items" not in st.session_state:
    st.session_state.hidden_items = {}

# ==========================================
# SCREEN 1: HOME SCREEN (Welcome / Landing)
# ==========================================
if st.session_state.navigation == "home":
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<div class='dashboard-card' style='text-align: center;'>", unsafe_allow_html=True)
        st.markdown("<h1>🎬 CineSpark</h1>", unsafe_allow_html=True)
        st.markdown("<p>Your AI-Powered Multi-Task Recommendation Engine</p>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚀 Get Started / Login", type="primary"):
            st.session_state.navigation = "auth"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

# ==========================================
# SCREEN 2: LOGIN / CREATE USER SCREEN (CRUD)
# ==========================================
elif st.session_state.navigation == "auth":
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<div class='dashboard-card'>", unsafe_allow_html=True)
        st.subheader("Account Access")

        tab_login, tab_create = st.tabs(["🔑 Login with User ID", "➕ Create New Profile"])

        with tab_login:
            login_id = st.text_input("Enter existing `reviewer_id` or User ID:", placeholder="e.g. AGSWFPUAVOIESVU...")
            if st.button("Log In"):
                if login_id.strip():
                    if login_id not in st.session_state.users:
                        st.session_state.users[login_id] = {"name": f"User {login_id[:6]}", "language": "English",
                                                            "watched": {}}
                    st.session_state.current_user = login_id
                    st.session_state.navigation = "dashboard"
                    st.rerun()
                else:
                    st.error("Please enter a valid ID.")

        with tab_create:
            new_id = st.text_input("Choose a unique User ID:", placeholder="e.g. user_adam_01")
            new_name = st.text_input("Profile Display Name:", placeholder="e.g. Adam Stonebridge")
            preferred_language = st.selectbox("Preferred Audio/Subtitle Language:",
                                              ["English", "Spanish", "French", "Japanese", "German", "Other"])

            if st.button("Create Profile & Start Setup"):
                if new_id.strip() and new_name.strip():
                    if new_id in st.session_state.users:
                        st.error("User ID already exists. Choose another or log in directly.")
                    else:
                        st.session_state.users[new_id] = {
                            "name": new_name,
                            "language": preferred_language,
                            "watched": {}
                        }
                        st.session_state.current_user = new_id
                        st.session_state.navigation = "onboarding"
                        st.rerun()
                else:
                    st.error("Please fill in all fields.")

        st.markdown("---")
        if st.button("⬅️ Back to Home"):
            st.session_state.navigation = "home"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

# ==========================================
# SCREEN 3: ONBOARDING / STARTING QUESTIONS
# ==========================================
elif st.session_state.navigation == "onboarding":
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<div class='dashboard-card'>", unsafe_allow_html=True)
        st.subheader("🎯 Tailor Your Recommendations")
        st.write(
            "Answer a few quick questions so our collaborative filtering and fallback models can calibrate your feed.")

        fav_genre = st.selectbox("What is your favorite primary category?",
                                 ["Comedy", "Documentary", "Kids", "Animation", "Action"])
        viewing_freq = st.slider("How many movies/shows do you watch per week?", 1, 15, 5)
        watchlist_seed = st.multiselect("Pick 2-3 shows you've already enjoyed:",
                                        [m["title"] for m in st.session_state.catalog])

        if st.button("Generate Recommendations", type="primary"):
            curr = st.session_state.current_user
            # Initialize seeds with default 5-star rating
            for show in watchlist_seed:
                st.session_state.users[curr]["watched"][show] = 5.0
            st.session_state.navigation = "dashboard"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

# ==========================================
# SCREEN 4: MAIN STREAMING DASHBOARD (CRUD + UI)
# ==========================================
elif st.session_state.navigation == "dashboard":
    curr_id = st.session_state.current_user
    user_info = st.session_state.users.get(curr_id, {"name": "Guest", "language": "English", "watched": {}})

    # Top Navbar Card
    st.markdown("<div class='dashboard-card'>", unsafe_allow_html=True)
    nav_col1, nav_col2, nav_col3 = st.columns([2, 4, 2])
    with nav_col1:
        st.markdown(f"### 📺 CineSpark")
    with nav_col2:
        st.markdown(f"**Active Profile:** `{user_info['name']}` | **Lang:** `{user_info['language']}`")
    with nav_col3:
        if st.button("🚪 Log Out / Switch"):
            st.session_state.current_user = None
            st.session_state.navigation = "auth"
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    # Sidebar CRUD Management Hub
    with st.sidebar:
        st.markdown("### ⚙️ Controls")
        st.markdown("---")
        st.subheader("➕ Add Watched Show")
        show_to_add = st.selectbox("Log show:", [m["title"] for m in st.session_state.catalog])

        # Rating selection prompt right after selecting show
        show_rating = st.slider("Rate this show (Stars):", 1.0, 5.0, 4.0, 0.5)

        if st.button("Mark as Watched & Rate"):
            st.session_state.users[curr_id]["watched"][show_to_add] = show_rating
            st.success(f"Logged '{show_to_add}' with {show_rating}★!")

        st.markdown("---")
        st.subheader("🗑️ Account Deletion")
        if st.button("Delete Profile", type="secondary"):
            del st.session_state.users[curr_id]
            st.session_state.current_user = None
            st.session_state.navigation = "home"
            st.rerun()

        st.markdown("---")
        st.info(f"ID: `{curr_id}`")

    # Main Streaming Rows inside a card container
    st.markdown("<div class='dashboard-card'>", unsafe_allow_html=True)
    st.subheader("🔥 Recommended For You")

    user_hidden = st.session_state.hidden_items.get(curr_id, [])
    active_catalog = [m for m in st.session_state.catalog if m["title"] not in user_hidden]

    cols = st.columns(min(len(active_catalog), 4) if active_catalog else 1)

    if not active_catalog:
        st.info("No recommendations available.")
    else:
        for idx, movie in enumerate(active_catalog):
            col_idx = idx % len(cols)
            with cols[col_idx]:
                st.image(movie["url"], use_container_width=True)
                st.caption(movie["title"])
                if st.button("🚫 Don't Recommend", key=f"hide_{movie['id']}_{idx}"):
                    if curr_id not in st.session_state.hidden_items:
                        st.session_state.hidden_items[curr_id] = []
                    st.session_state.hidden_items[curr_id].append(movie["title"])
                    st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    # History Section Card
    st.markdown("<div class='dashboard-card'>", unsafe_allow_html=True)
    st.subheader("📚 Recently Watched & Ratings History")
    watched_dict = user_info.get("watched", {})
    if watched_dict:
        for show_title, rating in watched_dict.items():
            st.text(f"• {show_title} — {rating} ★")
    else:
        st.write("No watch history logged yet.")
    st.markdown("</div>", unsafe_allow_html=True)
