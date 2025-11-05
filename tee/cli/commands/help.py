"""
Help command implementation.
"""


def cmd_help(args):
    """
    Show help information.
    
    Args:
        args: Parsed arguments (contains parser reference)
    """
    # The parser is passed via args from main.py
    parser = getattr(args, 'parser', None)
    if parser:
        parser.print_help()
    else:
        # Fallback if parser not available
        print("Use 'tcli <command> --help' for command-specific help")

