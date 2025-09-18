def calculate_score(value):
    """
    Converts a number from -1 to 1 into a score.
    -1 is converted to 100, 0 to 0, and anything above 0 is also 0.
    The score is linearly interpolated between -1 and 0.

    Args:
        value (float): A number between -1 and 1.

    Returns:
        float: The calculated score.

    # Example usage:
    # print(calculate_score(-1))  # Output: 100
    # print(calculate_score(0))   # Output: 0
    # print(calculate_score(0.5)) # Output: 0
    # print(calculate_score(-0.72))# Output: 72
    """
    if value < -1 or value > 1:
        raise ValueError("Input must be between -1 and 1.")
    
    if value >= 0:
        return 0
    else:
        return -100 * value  # Linear gradient from -1 to 0

