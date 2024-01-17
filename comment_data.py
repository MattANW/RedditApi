from typing import List, Iterable
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
from nltk.corpus import stopwords
from utils import TwoWayMap, is_english, filter_characters

skip_words = stopwords.words('english')

skip_wordss = ["is", "this", "them", "the", "to", "some", "of", "an", "there", "at", "my", "be", 
                        "use", "her", "his", "he", "she", "a", "which", "have", "would", "like", "been", 
                        "that", "make", "now", "in", "him", "and", "had", "by", "i", "was", "if", "find", 
                        "you", "do", "will", "we", "as", "on", "are", 'were', "all", "go", "out", "see", 
                        "not", "up", "for", "come", "your", "these", "so", "may", "made", "but", "say", 
                        "with", "much", "top", "or", "case", "said", "who", "it", "should", "how", "me",
                        "you're", "essentially", "mostly", "from", "alomst", "any", "am", "i'm", "👍", 
                        "your", "🫶", "❤️", "💀", "🔥", "🥹", "😊", "✅", "😍", "🏳️‍🌈", "🎂", "sorry",
                        "think", "bending", "really", "together", "won't", "sounds", "their", "they", "came",
                        "into", "get", "can", "give", "those", "don’t", "stops", "changes", "least", "about", 
                        "what", "well", "maybe", "even", "putting", "🤓", "🥺", "🥰", "💋", "😂", "😘", "😈",
                        "😏", "💰", "😡", "😅", "🍆", "👅", "💪", "💕", "short", "no", "um", "😜", "just", 
                        "also", "has", "😑", "🙋", "could", "🙇", "🥵", "🙏", "🎶", "😢", "🙉", "💅", "🤣", 
                        "sent", "over", "than", "wait", "days", "saw", "when", "take", "can't", "i'll", "way", 
                        "😁", "wasn't", "own", "more", "sort", "knew", "same", "got", "older", "actually", "😆",
                        "through", "off", "only", "while", "enough", "heard", "absolutely", "thank", "forgot", 
                        "already", "aint", "i've", "reason", "dying", "im", "then", "who's", "did", "suck", "shit", 
                        "clearly", "after", "last", "again", "time", "oh", "anything", "matter", "i'd", "every", 
                        "single", "noticed", "earlier", "stop", "being", "never", "our", "weren't", "handed", 
                        "because", "works", "happens", "why", "soon", "wanna", "whatever", "ready", "it's", "easily", 
                        "wants", "against", "everyday", "instead", "method", "yet", "therefore", "didn’t", "deal", 
                        "seems", "yeah", "hit", "told", "here", "used", "we'll", "often", "i’m", "late", "around", 
                        "wouldn't", "lol", "tell", "that's", "called", "still", "able", "looking", "idea", "where", 
                        "always", "please", "problematic", "recent", "like", "very", "neither", "usually", "us", "let", 
                        "full", "look", "does", "main", "ever", "sure", "its", "new"]

class WordData:
    current_id = 0
    word_ids = TwoWayMap(str, int)

    def __init__(self, word: str = "", wID: int = -1, frequency: int = 1, subjectivity: float = 0, polarity: float = 0, add_count: int = 1) -> None:
        if wID == -1:
            self.wID = WordData.word_ids.get(word)

            if not self.wID:
                self.wID = WordData.current_id
                
                WordData.word_ids.set(word, self.wID)
                WordData.current_id += 1
        else:
            self.wID = wID

        self.frequency = 1
        self.subjectivity = subjectivity
        self.polarity = polarity

        self.add_count = add_count

    def __add__(self, other) -> object:
        if self.wID != other.wID:
            print("WARNING U ARE ADDING TWO DIFFERENT WORDS!")
        return WordData(
            wID=self.wID,
            frequency=((self.frequency * self.add_count) + (other.frequency * other.add_count))/(self.add_count + other.add_count),
            subjectivity=((self.subjectivity * self.add_count) + (other.subjectivity * other.add_count))/(self.add_count + other.add_count),
            polarity=((self.polarity * self.add_count) + (other.polarity * other.add_count))/(self.add_count + other.add_count),
            add_count=self.add_count + other.add_count
            )

class CommentData:
    def __init__(self, comment: str, community_id: any, post: str) -> None:
        self._word_freqs = {}
        
        skip_words = stopwords.words('english') + skip_wordss + stopwords.fileids()

        sentiment = TextBlob(comment).sentiment
        
        for word in comment.lower().split(" "):
            word = filter_characters(word)
            if word in skip_words or not 1 <= len(word) <= 30:
                continue
            if self._word_freqs.get(word, None) is not None:
                self._word_freqs[word] += 1
            else:
                self._word_freqs[word] = 1

        self._polarity = sentiment.polarity
        self._subjectivity = sentiment.subjectivity

        self._community_id = community_id
        
    def is_empty(self) -> bool:
        return True if len(self._word_freqs) == 0 else False
                
    def get_words(self) -> Iterable[tuple]:
        for word in self._word_freqs:
            yield (word, self._word_freqs[word], self._polarity, self._subjectivity)

    def __str__(self) -> str:
        return f"Freqs: {self._word_freqs}, Polarity: {self._polarity}, Subjectivity: {self._subjectivity}, cID: {self._community_id}, pID: {self._post_id}"


class PostIds:
    current_id = 0
    ids = {}

    @classmethod
    def get_id(cls, post_name: str) -> int:
        if post_name in cls.ids:
            return cls.ids[post_name]
        else:
            cls.ids[post_name] = cls.current_id
            cls.current_id += 1

            return cls.current_id - 1
