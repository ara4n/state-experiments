#!/usr/bin/env python3
import sys
import re

def colorize_hex_string(hex_string):
    """
    Takes a 32-bit hex string and returns it with terminal color codes
    using the most significant 24 bits as RGB background color.
    """
    # Extract the most significant 24 bits (first 6 hex digits)
    rgb_hex = hex_string[:6]
    
    # Convert to RGB values
    r = int(rgb_hex[0:2], 16)
    g = int(rgb_hex[2:4], 16)
    b = int(rgb_hex[4:6], 16)
    
    # Calculate luminance to determine if we need light or dark text
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    text_color = "0;0;0" if luminance > 0.5 else "255;255;255"  # Black or white text
    
    # Create the colored string with background color and appropriate text color
    colored = f"\033[48;2;{r};{g};{b}m\033[38;2;{text_color}m{hex_string}\033[0m"
    
    return colored

def main():
    # Read all input
    content = sys.stdin.read()
    
    # Pattern to match 32-bit hex strings
    hex_pattern = r'\b[0-9A-Fa-f]{8}\b'
    
    # Replace each hex string with its colorized version
    colorized_content = re.sub(hex_pattern, lambda m: colorize_hex_string(m.group(0)), content)
    
    # Output the result
    print(colorized_content, end='')

if __name__ == "__main__":
    main()