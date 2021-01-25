from bs4 import BeautifulSoup
from nltk.stem.snowball import SnowballStemmer
from langdetect import detect, lang_detect_exception
import re


# TextProcessor class that is used for the different text manipulation tasks
class TextProcessor:
    # Using this list of words as the stop words
    # Note: Since the stop words are all in lower case,
    # the text's words that are going to be checked have to be in lower case as well
    stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll",
                  "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's",
                  'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs',
                  'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those',
                  'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
                  'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
                  'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before',
                  'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under',
                  'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any',
                  'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own',
                  'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should',
                  "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
                  "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven',
                  "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan',
                  "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn',
                  "wouldn't"]

    # Searching in a set is a lot faster than searching in a list
    # Converting the stop words to a set
    stop_words = set(stop_words)

    # Stemming words for english language
    stemmer = SnowballStemmer('english')

    # Constructor of the class that sets the stemming as True or False
    def __init__(self, stemming=False):
        self.stemming = stemming

    # Finds and returns all the links that start with "http" from the given html text
    @staticmethod
    def get_https_links_from_text(html_text):
        return re.findall('(?<=<a href=")https[^"]*', html_text)

    # Returns the title of the given html_text (if it exists)
    # and also an array with the useful words
    def get_cleaned_doc(self, html_text):
        # Getting the html file as a bs4 object
        soup = BeautifulSoup(html_text, "html.parser")

        # Get the title from the html text if it exists
        if soup.title is not None:
            doc_title = soup.title.get_text(separator=' ', strip=True)
        else:
            doc_title = None

        # Extract/Remove all script/Javascript and CSS styling code from the bs4 object
        for script in soup(["script", "style"]):
            script.extract()

        # Get the text from the bs4 object
        text = soup.get_text(separator=' ', strip=True)

        # If the language of this text in not English, raise a value error
        # (in order to abort this html file from the outside)
        lang = detect(text)
        if lang != 'en':
            raise lang_detect_exception.LangDetectException(code=lang != 'en', message="Non-english language, " + lang)

        # Remove non-english letters (Keeping numbers and english letters)
        # letters_only = re.sub("[^a-zA-Z]", " ", text)
        letters_only = re.sub("[^a-zA-Z0-9]+", " ", text)

        # Converting to lower case, split into individual words
        words = letters_only.lower().split()

        # Removing stop words, in order to keep only the useful words
        useful_words = [word for word in words if word not in TextProcessor.stop_words]

        # If stemming is used, applying stemming in order to break a word down into its' root
        if self.stemming:
            useful_words = [TextProcessor.stemmer.stem(word) for word in useful_words]

        return doc_title, useful_words
