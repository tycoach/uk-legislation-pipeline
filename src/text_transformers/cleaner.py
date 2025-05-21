import re
import logging
from typing import Dict, Any, List, Tuple
from bs4 import BeautifulSoup
from bs4.element import Comment


class LegislationCleaner:
    """
    Cleans raw HTML legislation content by:
    - Removing images and watermarks
    - Removing non-essential annotations
    - Extracting metadata
    - Converting HTML to structured text with metadata
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def clean(self, legislation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean legislation data from raw HTML.

        Args:
            legislation_data: Dictionary containing legislation metadata and html_content

        Returns:
            Cleaned legislation data with extracted content and additional metadata
        """
        if not legislation_data or 'html_content' not in legislation_data:
            self.logger.error("Missing HTML content in legislation data")
            return legislation_data

        try:
            html_content = legislation_data['html_content']
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract additional metadata
            metadata = self._extract_metadata(soup, legislation_data)

            # Clean the HTML content
            cleaned_soup = self._clean_html(soup)

            # Extract structured content (sections, paragraphs, etc.)
            structured_content = self._extract_structured_content(cleaned_soup)

            # Prepare cleaned data, preserving original keys and adding new fields
            cleaned_data = {
                **legislation_data,
                'metadata': metadata,
                'content': structured_content,
                'cleaned_html': str(cleaned_soup)
            }

            # Ensure unique ID is preserved in cleaned data
            cleaned_data['id'] = legislation_data.get('id', legislation_data.get('legislation_id'))or None

            # Remove the original HTML content to save space
            if 'html_content' in cleaned_data:
                del cleaned_data['html_content']

            self.logger.debug(f"Cleaning legislation with ID: {cleaned_data.get('id')} and title: {metadata.get('title')}")

            return cleaned_data

        except Exception as e:
            self.logger.error(f"Error cleaning legislation data: {str(e)}")
            return legislation_data

    def _clean_html(self, soup: BeautifulSoup) -> BeautifulSoup:
        """
        Clean HTML by removing images, watermarks, and non-essential elements.

        Args:
            soup: BeautifulSoup object with the HTML content

        Returns:
            Cleaned BeautifulSoup object
        """
        # Make a deep copy to avoid modifying the original
        soup_copy = BeautifulSoup(str(soup), 'html.parser')

        # Remove all images
        for img in soup_copy.find_all('img'):
            img.decompose()

        # Remove watermarks (typically in specific divs or with specific classes)
        for watermark in soup_copy.select('.watermark, .print-only, .crest'):
            watermark.decompose()

        # Remove scripts and styles
        for tag in soup_copy.find_all(['script', 'style']):
            tag.decompose()

        # Remove comments
        for comment in soup_copy.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Remove non-essential annotations (often in specific classes or spans)
        for annotation in soup_copy.select('.annotation, .editorial, .commentary, .note'):
            annotation.decompose()

        # Clean up any empty elements
        for elem in soup_copy.find_all():
            if elem.string is not None:
                elem.string = elem.string.strip()
            elif not elem.contents:
                elem.decompose()

        return soup_copy

    def _extract_metadata(self, soup: BeautifulSoup, existing_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract additional metadata from the legislation HTML.

        Args:
            soup: BeautifulSoup object with the HTML content
            existing_metadata: Existing metadata dictionary

        Returns:
            Enhanced metadata dictionary
        """
        metadata = {}

        # Start with existing metadata
        if existing_metadata:
            for k, v in existing_metadata.items():
                if k != 'html_content' and k != 'content':
                    metadata[k] = v

        # Extract document title
        title_elem = soup.select_one('h1.title, .title')
        if title_elem:
            metadata['title'] = title_elem.text.strip()

        # Extract dates
        enacted_date = soup.select_one('.enacted-date, .signedDate')
        if enacted_date:
            metadata['enacted_date'] = enacted_date.text.strip()

        coming_into_force_date = soup.select_one('.made-date, .comingIntoForce')
        if coming_into_force_date:
            metadata['coming_into_force_date'] = coming_into_force_date.text.strip()

        # Extract document number
        doc_number = soup.select_one('.doc-number, .documentNumber')
        if doc_number:
            metadata['document_number'] = doc_number.text.strip()

        # Extract document type (Act, Regulation, etc.)
        doc_type = soup.select_one('.legislation-type, .documentType')
        if doc_type:
            metadata['document_type'] = doc_type.text.strip()

        # Extract full document subtitle
        subtitle = soup.select_one('.legislation-subtitle, .documentSubtitle')
        if subtitle:
            metadata['subtitle'] = subtitle.text.strip()

        # Extract ISBN if available
        isbn_elem = soup.find(string=re.compile(r'ISBN|International Standard Book Number'))
        if isbn_elem:
            isbn_match = re.search(r'(?:ISBN|International Standard Book Number)[:\s]*([\d\-X]+)', isbn_elem)
            if isbn_match:
                metadata['isbn'] = isbn_match.group(1).strip()

        return metadata

    def _extract_structured_content(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract structured content from the cleaned HTML.

        Args:
            soup: Cleaned BeautifulSoup object

        Returns:
            List of content sections with their text and metadata
        """
        # Main content is typically in specific divs or sections
        content_container = soup.select_one('#content, .legislation-body, .primaryContent, main')
        if not content_container:
            content_container = soup

        # Extract sections, parts, chapters
        sections = []
        
        # Find all content dividers (sections, parts, chapters)
        dividers = content_container.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        # If no dividers, treat whole document as one section
        if not dividers:
            text = self._get_clean_text(content_container)
            if text:
                sections.append({
                    'section_type': 'document',
                    'section_number': '1',
                    'section_title': 'Main Content',
                    'text': text
                })
            return sections
            
        current_section = None
        
        # Process each section
        for i, divider in enumerate(dividers):
            section_type, section_number, section_title = self._parse_section_header(divider)
            
            # Complete previous section
            if current_section:
                # Get content between this divider and the previous one
                content = self._gather_content_between(current_section['element'], divider)
                current_section['text'] = content
                # Remove the element reference before adding to sections
                element = current_section.pop('element', None)
                sections.append(current_section)
            
            # Start new section
            current_section = {
                'section_type': section_type,
                'section_number': section_number,
                'section_title': section_title,
                'element': divider
            }
            
            # If this is the last divider, get all content after it
            if i == len(dividers) - 1:
                content = self._gather_content_after(divider)
                current_section['text'] = content
                element = current_section.pop('element', None)
                sections.append(current_section)
                
        return sections
    
    def _parse_section_header(self, header_elem) -> Tuple[str, str, str]:
        """
        Parse section headers to extract type, number, and title.
        
        Args:
            header_elem: The header element (h1, h2, etc.)
            
        Returns:
            Tuple of (section_type, section_number, section_title)
        """
        header_text = header_elem.text.strip()
        
        # Try to identify section types and numbers
        section_match = re.match(r'^(Part|Chapter|Section|Regulation|Article|Schedule)\s+([\w\d\.]+)[:\.\s]*(.*)', header_text, re.IGNORECASE)
        
        if section_match:
            section_type = section_match.group(1).lower()
            section_number = section_match.group(2)
            section_title = section_match.group(3).strip()
        else:
            # Fallback for headers that don't match the pattern
            tag_name = header_elem.name
            section_type = f"level_{tag_name[1]}"  # e.g., "level_2" for h2
            section_number = ""
            section_title = header_text
            
        return section_type, section_number, section_title
    
    def _gather_content_between(self, start_elem, end_elem) -> str:
        """
        Gather all text content between two elements.
        
        Args:
            start_elem: Starting element
            end_elem: Ending element
            
        Returns:
            Clean text content between elements
        """
        text_parts = []
        current = start_elem.next_sibling
        
        while current and current != end_elem:
            if hasattr(current, 'text'):
                text = self._get_clean_text(current)
                if text:
                    text_parts.append(text)
            current = current.next_sibling
            
        return '\n\n'.join(text_parts)
    
    def _gather_content_after(self, elem) -> str:
        """
        Gather all text content after an element until the end of its parent.
        
        Args:
            elem: Starting element
            
        Returns:
            Clean text content after the element
        """
        text_parts = []
        current = elem.next_sibling
        
        while current:
            if hasattr(current, 'text'):
                text = self._get_clean_text(current)
                if text:
                    text_parts.append(text)
            current = current.next_sibling
            
        return '\n\n'.join(text_parts)
    
    def _get_clean_text(self, elem) -> str:
        """
        Get clean text from an element, removing excess whitespace.
        
        Args:
            elem: HTML element
            
        Returns:
            Clean text content
        """
        if not elem:
            return ""
            
        text = elem.get_text(separator=' ', strip=True)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text
