"""
Branch-specific UI theming utilities.
Handles color themes, logos, and custom styling for different branches.
"""


def get_branch_theme_css(branch_info):
    """
    Generate CSS variables for a branch's theme.

    Args:
        branch_info: Dictionary with branch color and theme information

    Returns:
        CSS string with CSS custom properties for the branch theme
    """
    if not branch_info:
        # Default theme
        return """
        :root {
            --primary-color: #dc2626;
            --secondary-color: #ef4444;
            --accent-color: #10b981;
            --text-color: #ffffff;
            --background-color: #0a0a0a;
            --primary-rgb: 220, 38, 38;
            --secondary-rgb: 239, 68, 68;
            --accent-rgb: 16, 185, 129;
        }
        """

    # Extract colors from branch info
    primary = branch_info.get("primary_color", "#dc2626")
    secondary = branch_info.get("secondary_color", "#ef4444")
    accent = branch_info.get("accent_color", "#10b981")
    text = branch_info.get("text_color", "#ffffff")
    background = branch_info.get("background_color", "#0a0a0a")

    # Convert hex to RGB for use in rgba()
    def hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip("#")
        return ",".join(str(int(hex_color[i : i + 2], 16)) for i in (0, 2, 4))

    primary_rgb = hex_to_rgb(primary)
    secondary_rgb = hex_to_rgb(secondary)
    accent_rgb = hex_to_rgb(accent)

    css = f"""
    :root {{
        --primary-color: {primary};
        --secondary-color: {secondary};
        --accent-color: {accent};
        --text-color: {text};
        --background-color: {background};
        --primary-rgb: {primary_rgb};
        --secondary-rgb: {secondary_rgb};
        --accent-rgb: {accent_rgb};
    }}
    
    /* Branch-specific overrides */
    .branch-header {{ 
        background: linear-gradient(135deg, {primary}, {secondary});
        color: {text};
    }}
    
    .sidebar-item.active {{ 
        border-color: {primary};
        background: rgba({primary_rgb}, 0.12);
    }}
    
    .metric-card:hover {{ 
        box-shadow: 0 12px 30px rgba({primary_rgb}, 0.3);
    }}
    
    .progress-fill {{ 
        background: linear-gradient(90deg, {primary}, {secondary});
    }}
    
    .btn-primary {{
        background-color: {primary};
    }}
    
    .btn-primary:hover {{
        background-color: {secondary};
    }}
    
    .badge-success {{
        background: rgba({accent_rgb}, 0.2);
        color: {accent};
        border-color: {accent};
    }}
    
    .table-hover tbody tr:hover {{
        background-color: rgba({primary_rgb}, 0.05);
    }}
    """

    # Add custom CSS if provided
    if branch_info.get("custom_css"):
        css += f"\n    /* Custom CSS for branch */\n    {branch_info['custom_css']}"

    return css


def get_branch_logo_html(branch_info):
    """
    Generate HTML for branch logo/branding.

    Args:
        branch_info: Dictionary with branch information

    Returns:
        HTML string for branch logo
    """
    if not branch_info:
        return '<span class="text-red-600 font-bold">J&T Express</span>'

    logo_url = branch_info.get("logo_url")
    branch_name = branch_info.get("branch_name", "Branch")

    if logo_url:
        return f'<img src="{logo_url}" alt="{branch_name}" class="h-8 w-auto object-contain">'
    else:
        return (
            f'<span class="font-bold" style="color: {branch_info.get("primary_color", "#dc2626")}">{branch_name}</span>'
        )


def get_branch_banner_style(branch_info):
    """
    Generate inline style for branch banner.

    Args:
        branch_info: Dictionary with branch information

    Returns:
        CSS style string for banner
    """
    if not branch_info:
        return "background: linear-gradient(135deg, #0a0a0a, #1f2937);"

    primary = branch_info.get("primary_color", "#dc2626")
    secondary = branch_info.get("secondary_color", "#ef4444")
    background = branch_info.get("background_color", "#0a0a0a")

    return f"background: linear-gradient(135deg, {background}, {secondary}); border-bottom: 3px solid {primary};"


def generate_theme_palette(branch_info):
    """
    Generate a complete theme palette for a branch.

    Returns:
        Dictionary with all theme colors and utilities
    """
    palette = {
        "primary": branch_info.get("primary_color", "#dc2626"),
        "secondary": branch_info.get("secondary_color", "#ef4444"),
        "accent": branch_info.get("accent_color", "#10b981"),
        "text": branch_info.get("text_color", "#ffffff"),
        "background": branch_info.get("background_color", "#0a0a0a"),
        "logo_url": branch_info.get("logo_url"),
        "banner_url": branch_info.get("banner_url"),
        "theme_name": branch_info.get("theme_name", "default"),
    }

    return palette
