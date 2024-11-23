import sys
import cv2
import numpy as np
import pdfplumber
import tabula

from .utils import filter_rectangles


class TableExtractor():
    def __init__(self, pdfPath) -> None:
        self.pdfPath = pdfPath
        self.pdf = pdfplumber.open(pdfPath)
        self.DPI = 72
        self.BBOX_HORIZONTAL_PADDING = 0
        self.BBOX_VERTICAL_PADDING = 0
        # This value might need tuning.
        self.DILATION_ITERATION = 4
    
    def convert_image_to_grayscale(self, image):
        return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    def threshold_image(self, grayscale_image):
        return cv2.threshold(grayscale_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    def invert_image(self, thresholded_image):
        return cv2.bitwise_not(thresholded_image)
    def dilate_image(self, inverted_image, iterations=4):
        return cv2.dilate(inverted_image, None, iterations=iterations)
    
    def find_contours(self, dilated_image, original_image):
        result = cv2.findContours(dilated_image, mode=cv2.RETR_EXTERNAL, method=cv2.CHAIN_APPROX_SIMPLE)
        contours = result[0]
        image_with_contours_drawn = original_image.copy()
        cv2.drawContours(image_with_contours_drawn, contours, -1, (0, 255, 0), 3)
        return contours, image_with_contours_drawn
    
    def get_lines_in_page(self, inverted_image):
        # Detect vertical lines
        hor = np.array([[1,1,1,]])
        vertical_lines_eroded_image = cv2.erode(inverted_image, hor, iterations=10)
        vertical_lines_eroded_image = cv2.dilate(vertical_lines_eroded_image, hor, iterations=10)

        # Detect horizontal lines
        ver = np.array([[1],
            [1],
            [1],
            [1],
            [1],
            [1],
            [1]])
        horizontal_lines_eroded_image = cv2.erode(inverted_image, ver, iterations=10)
        horizontal_lines_eroded_image = cv2.dilate(horizontal_lines_eroded_image, ver, iterations=10)

        # Form combined line structures
        combined_image = cv2.add(vertical_lines_eroded_image, horizontal_lines_eroded_image)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        combined_image_dilated = cv2.dilate(combined_image, kernel, iterations=2)

        return combined_image_dilated

    def bbox_to_pdf_area(self, bbox):
        """
        Accepts rect co-ordinates in (y1, x1, y2, x2) format
        """
        # return tuple([x*72/self.DPI for x in bbox])
        return bbox
    
    def convert_contours_to_bounding_boxes(self, contours, original_image):
        bounding_boxes = []
        # Might need to tweak this
        sizeThreshold = 0.002
        image_with_all_bounding_boxes = original_image.copy()
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if w*h*sizeThreshold < 1:
                continue
            bounding_boxes.append((x, y, w, h))
            image_with_all_bounding_boxes = cv2.rectangle(image_with_all_bounding_boxes, (x, y), (x + w, y + h), (0, 255, 0), 5)
        return bounding_boxes, image_with_all_bounding_boxes
    
    def filter_aspect_ratio(self, bbox):
        possible_tables = []
        for bb in bbox:
            x, y, w, h = bb
            aspect_ratio = w / h
            print(aspect_ratio)
            if aspect_ratio > 12 or h < 25:
                print("Skipping small bbox")
            else:
                possible_tables.append((x, y, w, h))
        return possible_tables
    
    def merge_columnar_bboxes(self, bbox):
        bbox.sort(key = lambda i:(i[1], i[0]))
        prev_x, prev_y, prev_w, prev_h = (0, 0, 0, 0)

        all_siblings = []
        horizontal_siblings = []
        sibling_map = {}

        for bb in bbox:
            x, y, w, h = bb
            # The top line of the box comes before ending line of prev box and there is at leaset 30% vertical region share between them
            if (prev_h + prev_y) > y and ((((prev_h + prev_y) - y) / float(h)) > 0.3):
                horizontal_siblings.append(bb)
            else:
                if len(horizontal_siblings) > 1:
                    all_siblings.append(horizontal_siblings)
                horizontal_siblings = [bb]
            prev_x, prev_y, prev_w, prev_h = bb
        
        if len(horizontal_siblings) > 1:
            all_siblings.append(horizontal_siblings)
        
        confident_table_boxes = []
        for sibling in all_siblings:
            min_x, min_y, max_x, max_y = (sys.maxsize, sys.maxsize, 0, 0)
            for bb in sibling:
                x, y, w, h = bb

                min_x = min(min_x, x)
                min_y = min(min_y, y)

                max_x = max(max_x, w + x)
                max_y = max(max_y, h + y)
                try:
                    bbox.remove(bb)
                except:
                    print("bbox doesn't exist in the list or it's already deleted")

            confident_table_boxes.append((min_x, min_y, max_x - min_x, max_y - min_y))
            sibling_map[(min_x, min_y, max_x - min_x, max_y - min_y)] = sibling
        return confident_table_boxes, sibling_map
    

    def check_vertical_sibling_alignment(self, mbox, sibling_map, prev_siblings):
        horizontal_siblings = sibling_map[mbox]
        horizontal_siblings.sort(key = lambda i:(i[1], i[0]))
        prev_siblings.sort(key = lambda i:(i[1], i[0]))

        match_count = 0
        min_sibling_count = min(len(horizontal_siblings), len(prev_siblings))
        
        for i in range(min_sibling_count):
            # Prev sibling on same index have similar ending x co-ordinate as beginning of current sibling
            if abs((prev_siblings[i][0] - horizontal_siblings[i][0]) < 5):
                match_count += 1
        # There should be no more than 30% difference between total sibling count and how many aligns
        return abs( (match_count - min_sibling_count) / float(min_sibling_count) ) <= 0.30
        
    def merge_vertical_bboxes(self, sibling_map):
        merged_bbox_gridless = list(sibling_map.keys())
        merged_bbox_gridless.sort(key = lambda i:(i[1], i[0]))
        prev_x, prev_y, prev_w, prev_h = (0, 0, 0, 0)
        prev_siblings = []
        all_vertical_siblings = []
        vertical_sibling = []

        
        for mbox in merged_bbox_gridless:
            # If box is created merging multiple smaller boxes but still removed due to aspect ratio
            # if mbox not in semigrid_tables:
            x, y, w, h = mbox
            if (abs(prev_w - w) < 20 or abs(prev_h - h) < 15) and (
                abs(prev_x - x) < 20 or abs((prev_x + prev_w) - (w + x)) < 20) and (
                    abs((prev_y + prev_h) - y) < 30):
                vertical_sibling.append(mbox)
            else:
                # If all conditions checked against at least 3 consecutive boxes, add them
                if len(vertical_sibling) > 2:
                    all_vertical_siblings.append(vertical_sibling)
                # If it have exactly two consecutive boxes, additional checks are required
                elif len(vertical_sibling) == 2:
                    # There should be no more than 30% difference between total sibling count and how many aligns
                    if self.check_vertical_sibling_alignment(mbox, sibling_map, prev_siblings):
                        all_vertical_siblings.append(vertical_sibling)

                vertical_sibling = [mbox]
                
            prev_x, prev_y, prev_w, prev_h = mbox
            prev_siblings = sibling_map[mbox]
        

        # If all conditions checked against at least 3 consecutive boxes, add them
        if len(vertical_sibling) > 2:
            all_vertical_siblings.append(vertical_sibling)
        # If it have exactly two consecutive boxes, additional checks are required
        elif len(vertical_sibling) == 2:
            # There should be no more than 30% difference between total sibling count and how many aligns
            if self.check_vertical_sibling_alignment(mbox, sibling_map, prev_siblings):
                all_vertical_siblings.append(vertical_sibling)

        confident_table_boxes = []
        for siblings in all_vertical_siblings:
            min_x, min_y, max_x, max_y = (sys.maxsize, sys.maxsize, 0, 0)
            for bb in siblings:
                x, y, w, h = bb

                min_x = min(min_x, x)
                min_y = min(min_y, y)

                max_x = max(max_x, w + x)
                max_y = max(max_y, h + y)

            confident_table_boxes.append((min_x, min_y, max_x - min_x, max_y - min_y))

        return confident_table_boxes
    

    def get_table_locations_from_page(self, page_no=0):
        if page_no >= len(self.pdf.pages):
            return [], []
        page = self.pdf.pages[page_no]
        im = page.to_image().original
        im.save("/home/dhivakar/pdf_table_extractor/output_image.png")
        image = np.array(im)
        gs_img = self.convert_image_to_grayscale(image)
        th_img = self.threshold_image(gs_img)
        inv_img = self.invert_image(th_img)

        # Get all lines from page
        page_lines = self.get_lines_in_page(inv_img)
        page_without_lines = cv2.subtract(inv_img, page_lines)

        # Parameter tweeaking might be needed
        d_img = self.dilate_image(inv_img, iterations=self.DILATION_ITERATION)

        conts, cviz = self.find_contours(d_img, image)
        bbox, bbox_img = self.convert_contours_to_bounding_boxes(conts, image)
        bbox, sibling_map = self.merge_columnar_bboxes(bbox)
        gridless_tables = self.filter_aspect_ratio(bbox)

        # Find bounding boxes of grids
        grid_conts, _ = self.find_contours(page_lines, image)
        grid_bbox, grid_bbox_img = self.convert_contours_to_bounding_boxes(grid_conts, image)
        grid_tables = self.filter_aspect_ratio(grid_bbox)

        # Find bounding boxes if all of the lines were removed
        d_img_wo_lines = self.dilate_image(page_without_lines, iterations=self.DILATION_ITERATION)
        semigrid_conts, _ = self.find_contours(d_img_wo_lines, image)
        semigrid_bbox, semigrid_bbox_img = self.convert_contours_to_bounding_boxes(semigrid_conts, image)
        semigrid_bbox, semigrid_sibling_map = self.merge_columnar_bboxes(semigrid_bbox)
        semigrid_tables = self.filter_aspect_ratio(semigrid_bbox)


        vertically_merged_semigrid = self.merge_vertical_bboxes(semigrid_sibling_map)
        semigrid_tables += vertically_merged_semigrid
        vertically_merged_gridless = self.merge_vertical_bboxes(sibling_map)
        gridless_tables += vertically_merged_gridless

        gridless_tables = gridless_tables + semigrid_tables

        # Try to remove nested boxes
        grid_tables = filter_rectangles(grid_tables)
        gridless_tables = filter_rectangles(gridless_tables)
        nonoverlapping_boxes = filter_rectangles(gridless_tables + grid_tables)

        # only allow non overlapping boxes to exist
        grid_tables = [x for x in grid_tables if x in nonoverlapping_boxes]
        gridless_tables = [x for x in gridless_tables if x in nonoverlapping_boxes]

        return grid_tables, gridless_tables


    def extract_tables_in_page(self, page_no=0):
        if len(self.pdf.pages) <= page_no:
            return []
        grid_tables, gridless_tables = self.get_table_locations_from_page(page_no)
        if grid_tables:
            grid_tables = [self.bbox_to_pdf_area((bbox[1] - self.BBOX_VERTICAL_PADDING, bbox[0] - self.BBOX_HORIZONTAL_PADDING, bbox[1] + bbox[3] + self.BBOX_VERTICAL_PADDING, bbox[0] + bbox[2] + self.BBOX_HORIZONTAL_PADDING)) for bbox in grid_tables]
        if gridless_tables:
            gridless_tables = [self.bbox_to_pdf_area((bbox[1] - self.BBOX_VERTICAL_PADDING, bbox[0] - self.BBOX_HORIZONTAL_PADDING, bbox[1] + bbox[3] + self.BBOX_VERTICAL_PADDING, bbox[0] + bbox[2] + self.BBOX_HORIZONTAL_PADDING)) for bbox in gridless_tables]

        grid_tables.sort()
        gridless_tables.sort()

        # print(f"Area: {confident_tables+possible_tables}")
        # dfs = tabula.read_pdf(self.pdfPath, multiple_tables = True, pages=page_no, area=confident_tables+possible_tables, stream=True)
        dfs = []
        for tab_area in grid_tables:
            print(f"Area: {tab_area} of page no {page_no + 1}")
            df = tabula.read_pdf(self.pdfPath, multiple_tables = True, pages=(page_no+1), area=tab_area, stream=True, guess=False, output_format='dataframe', pandas_options={"dtype": str})
            if df:
                print(df)
                dfs.append(df[0])
        
        for tab_area in gridless_tables:
            print(f"Area: {tab_area} of page no {page_no + 1}")
            df = tabula.read_pdf(self.pdfPath, multiple_tables = True, pages=(page_no+1), area=tab_area, stream=True, guess=False, output_format='dataframe', pandas_options={"dtype": str})
            if df:
                print(df)
                dfs.append(df[0])
        return dfs
    
    def extract_tables(self, page_range = range(1)):
        df_map = {}
        for page in page_range:
            try:
                dfs = self.extract_tables_in_page(page)
                for df in dfs:
                #     # print(df.head())
                    if len(df):
                        df_map[page] = dfs
            except Exception as e:
                print("FATAL: Unable to extract table from page due to: %s" % e)
        return df_map
    
    def close(self):
        return self.pdf.close()