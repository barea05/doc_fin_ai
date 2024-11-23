import time
from functools import wraps
def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} took {end_time - start_time} seconds to execute.")
        return result
    return wrapper

def find_intersection(rect1, rect2):
    x1, y1, w1, h1 = rect1
    x2, y2, w2, h2 = rect2
    
    # Calculate coordinates of intersection
    inter_left = max(x1, x2)
    inter_right = min(x1 + w1, x2 + w2)
    inter_top = max(y1, y2)
    inter_bottom = min(y1 + h1, y2 + h2)
    
    # Check if there is intersection
    if inter_left < inter_right and inter_top < inter_bottom:
        inter_width = inter_right - inter_left
        inter_height = inter_bottom - inter_top
        
        # Calculate areas of rectangles and intersection
        area_rect1 = w1 * h1
        area_rect2 = w2 * h2
        area_intersection = inter_width * inter_height
        
        # Calculate overlap ratio
        overlap_ratio = area_intersection / min(area_rect1, area_rect2)
        
        return overlap_ratio
    
    else:
        return 0.0  # No intersection

def is_rect1_inside_rect2(rect1, rect2):
    x1, y1, w1, h1 = rect1
    x2, y2, w2, h2 = rect2
    
    return x2 <= x1 and y2 <= y1 and (x2 + w2) >= (x1 + w1) and (y2 + h2) >= (y1 + h1)

def filter_rectangles(rectangles):
    rectangles.sort(key=lambda rect: rect[2] * rect[3], reverse=True)  # Sort by area (width * height), largest to smallest
    
    n = len(rectangles)
    is_inside = [False] * n
    to_remove = set()
    
    for i in range(n):
        if i in to_remove:
            continue
        
        for j in range(i + 1, n):
            if j in to_remove:
                continue
            
            overlap_ratio = find_intersection(rectangles[i], rectangles[j])
            
            if overlap_ratio > 0.8:
                to_remove.add(j)
            elif is_rect1_inside_rect2(rectangles[i], rectangles[j]):
                is_inside[i] = True
                break
    
    filtered_rectangles = [rectangles[i] for i in range(n) if i not in to_remove and not is_inside[i]]
    return filtered_rectangles
