from streamlit_option_menu import option_menu

selected = option_menu(
    menu_title=None,
    options=["Home", "Upload", "Settings"],
    icons=["house", "cloud-upload", "gear"],
    menu_icon="cast",
    orientation="horizontal",
)