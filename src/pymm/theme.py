"""Unified theme system for PythonMetaMap UI components

Light, colorful theme inspired by shadcn/ui with vibrant accents.
"""

# Color palette - Light, colorful theme
COLORS = {
    # Primary colors - vibrant and modern
    'primary': 'bright_blue',        # Bright blue for primary elements
    'secondary': 'cyan',             # Cyan for secondary elements
    'success': 'bright_green',       # Vibrant green for success
    'warning': 'yellow',             # Bright yellow/orange for warnings
    'error': 'bright_red',           # Bright red for errors
    'info': 'bright_cyan',           # Light cyan for info

    # Accent colors - colorful palette
    'accent1': 'bright_magenta',     # Purple accent
    'accent2': 'bright_cyan',        # Cyan accent
    'accent3': 'bright_green',       # Green accent
    'accent4': 'bright_yellow',      # Yellow accent

    # Background colors - removed to blend with terminal
    'bg_primary': '',        # No background, blend with terminal
    'bg_secondary': '',      # No background, blend with terminal
    'bg_accent': '',         # No background, blend with terminal
    'bg_blue': '',           # No background, blend with terminal
    'bg_green': '',          # No background, blend with terminal

    # Text colors - dark on light with color options
    'text_primary': 'black',         # Black for primary text
    'text_secondary': 'bright_black',  # Dark gray for secondary
    'text_dim': 'dim black',         # Lighter gray for dim text
    'text_muted': 'bright_black',    # Muted gray
    'text_accent': 'blue',           # Blue accent text

    # Status colors - vibrant
    'running': 'bright_green',
    'paused': 'bright_yellow',
    'stopped': 'bright_red',
    'idle': 'bright_cyan',

    # Progress colors - gradient effect
    'progress_low': 'bright_red',
    'progress_mid': 'bright_yellow',
    'progress_high': 'bright_green',
    'progress_complete': 'bright_blue',
}

# Box styles - clean with subtle shadows
BOX_STYLES = {
    'primary': 'ROUNDED',        # Rounded corners for modern look
    'secondary': 'SIMPLE',       # Simple borders
    'minimal': 'MINIMAL',        # Minimal style
    'heavy': 'DOUBLE',           # Double borders for emphasis
    'shadow': 'HEAVY',           # Heavy for shadow effect
}

# Panel styles - colorful and modern
PANEL_STYLES = {
    'default': {
        'box': 'ROUNDED',
        'style': '',     # No background, blend with terminal
        'padding': (1, 2),       # Good whitespace
        'border_style': 'bright_blue'  # Blue borders
    },
    'header': {
        'box': 'SIMPLE',
        'style': 'bold blue',
        'padding': (1, 2),
        'border_style': 'bright_cyan'
    },
    'footer': {
        'box': 'MINIMAL',        # Minimal border for footer
        'style': '',
        'padding': (1, 2),
        'border_style': 'bright_black'
    },
    'card': {
        'box': 'ROUNDED',
        'style': '',
        'width': 22,             # Wider for better content
        'border_style': 'bright_cyan',
        'padding': (1, 1)
    },
    'content': {
        'box': 'SIMPLE',
        'style': '',
        'padding': (1, 2),
        'border_style': 'bright_blue'
    },
    'success': {
        'box': 'ROUNDED',
        'style': 'bright_green',
        'padding': (1, 2),
        'border_style': 'bright_green'
    },
    'error': {
        'box': 'ROUNDED',
        'style': 'bright_red',
        'padding': (1, 2),
        'border_style': 'bright_red'
    },
    'warning': {
        'box': 'ROUNDED',
        'style': 'yellow',
        'padding': (1, 2),
        'border_style': 'bright_yellow'
    },
    'info': {
        'box': 'ROUNDED',
        'style': 'bright_cyan',
        'padding': (1, 2),
        'border_style': 'bright_cyan'
    }
}

# Progress bar styles - colorful gradients
PROGRESS_STYLES = {
    'filled': '█',          # Full blocks for modern look
    'partial': '▓',         # Medium shade
    'empty': '░',           # Light shade
    'gradient': ['▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'],  # Smooth gradient
    'colors': {
        'low': 'bright_red',        # Red for low progress
        'medium': 'bright_yellow',  # Yellow for medium
        'high': 'bright_green',     # Green for high
        'complete': 'bright_blue'   # Blue for complete
    }
}

# Table styles - colorful and modern
TABLE_STYLES = {
    'header_style': 'bold bright_blue',  # Blue headers
    'row_style': '',                     # Default terminal colors
    'alt_row_style': 'dim',              # Dimmed for alternating rows
    'border_style': 'bright_cyan',                # Cyan borders
    'title_style': 'bold bright_blue'             # Bold blue titles
}

# Icon mappings - professional unicode symbols
ICONS = {
    # File types - professional symbols
    'file': '▪',
    'folder': '▸',
    'text': '◆',
    'code': '▬',
    'data': '■',
    'log': '▫',

    # Status - professional indicators
    'success': '✓',
    'error': '✗',
    'warning': '⚠',
    'info': 'ℹ',
    'running': '►',
    'paused': '║',
    'stopped': '■',

    # Progress indicators
    'spinner': ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'],
    'dot': '•',
    'arrow': '▸',
    'check': '✓',
    'cross': '✗',

    # Navigation - professional arrows
    'up': '▲',
    'down': '▼',
    'left': '◄',
    'right': '►',
    'enter': '↵',
}


def get_color(name: str) -> str:
    """Get color by name with fallback"""
    return COLORS.get(name, 'white')


def get_panel_style(style_name: str = 'default') -> dict:
    """Get panel style configuration"""
    return PANEL_STYLES.get(style_name, PANEL_STYLES['default'])


def format_progress_bar(
        percent: float,
        width: int = 20,
        color_override: str = None) -> str:
    """Create a colorful gradient progress bar"""
    filled = int(width * percent / 100)
    empty = width - filled

    # Determine color based on percentage if not overridden
    if not color_override:
        if percent < 25:
            color = PROGRESS_STYLES['colors']['low']
        elif percent < 50:
            color = PROGRESS_STYLES['colors']['medium']
        elif percent < 100:
            color = PROGRESS_STYLES['colors']['high']
        else:
            color = PROGRESS_STYLES['colors']['complete']
    else:
        color = color_override

    # Create gradient bar with smooth transition
    bar = f"[{color}]"
    bar += PROGRESS_STYLES['filled'] * filled
    bar += f"[/{color}]"

    # Add empty part with light shade
    if empty > 0:
        bar += f"[bright_black]{PROGRESS_STYLES['empty'] * empty}[/bright_black]"

    return bar


def format_status(status: str) -> str:
    """Format status text with vibrant colors"""
    status_lower = status.lower()

    # Use vibrant colors for status
    if 'run' in status_lower or 'active' in status_lower:
        return f"[{COLORS['running']}]{ICONS['running']} {status}[/{COLORS['running']}]"
    elif 'pause' in status_lower:
        return f"[{COLORS['paused']}]{ICONS['paused']} {status}[/{COLORS['paused']}]"
    elif 'stop' in status_lower:
        return f"[{COLORS['stopped']}]{ICONS['stopped']} {status}[/{COLORS['stopped']}]"
    elif 'fail' in status_lower or 'error' in status_lower:
        return f"[{COLORS['error']}]{ICONS['error']} {status}[/{COLORS['error']}]"
    elif 'success' in status_lower or 'complete' in status_lower:
        return f"[{COLORS['success']}]{ICONS['success']} {status}[/{COLORS['success']}]"
    else:
        return f"[{COLORS['info']}]{ICONS['info']} {status}[/{COLORS['info']}]"


def get_progress_color(percent: float) -> str:
    """Get color based on progress percentage"""
    if percent < 25:
        return PROGRESS_STYLES['colors']['low']
    elif percent < 50:
        return PROGRESS_STYLES['colors']['medium']
    elif percent < 100:
        return PROGRESS_STYLES['colors']['high']
    else:
        return PROGRESS_STYLES['colors']['complete']


def apply_theme(obj):
    """Apply theme to a rich object (Panel, Table, etc.)"""
    from rich.panel import Panel
    from rich.table import Table

    if isinstance(obj, Panel):
        # Apply default panel styling if not already styled
        if not hasattr(obj, '_theme_applied'):
            style = get_panel_style()
            obj.box = getattr(__import__('rich.box'), style['box'])
            obj.style = style['style']
            obj.padding = style['padding']
            obj._theme_applied = True

    elif isinstance(obj, Table):
        # Apply table styling
        if not hasattr(obj, '_theme_applied'):
            obj.title_style = TABLE_STYLES['title_style']
            obj.header_style = TABLE_STYLES['header_style']
            obj.border_style = TABLE_STYLES['border_style']
            obj._theme_applied = True

    return obj
