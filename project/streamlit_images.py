import streamlit as st

def render_movie_card(title, poster_url):
    try:
        # Try displaying the extracted poster
        st.image(poster_url, caption=title, use_container_width=True)
    except Exception:
        # Fallback placeholder if the CDN blocks the request
        st.image("https://via.placeholder.com/300x450?text=Poster+Unavailable", caption=title, use_container_width=True)


st.title("Movie Poster Rendering Test")

# Mock data mimicking your extracted parquet lookup
sample_movies = [
    {
        "title": "PBS KIDS: Secret Superheroes!",
        "url": "https://m.media-amazon.com/images/I/51hwsu9AuGL.jpg"
    },
    {
        "title": "Timmy Time",
        "url": "https://images-na.ssl-images-amazon.com/images/S/pv-target-images/14793d49cfb72847f74c85cf009d1c2966bdb2c3a3c06911a82f83372bcd07be._RI_SX300_FMwebp_.jpg"
    }
]

cols = st.columns(len(sample_movies))
for col, movie in zip(cols, sample_movies):
    with col:
        st.image(movie["url"], caption=movie["title"], use_container_width=True)