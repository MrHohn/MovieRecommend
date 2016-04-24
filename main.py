import json
import os.path
import requests
import threading
import urllib.request
import tkinter as tk
import DataService
from PIL import Image, ImageTk
from functools import partial

BGCOLOR = 'white'
TWICOLOR = '#4099FF'
WINWIDTH = 800
WINHEIGHT = 600
AUTOCOMPLETE_DELAY = 5
UPDATE_FREQUENCY = 100 #milliseconds

FONT_H1 = ("Helvetica", 18, "bold")
FONT_H2 = ("Helvetica", 12, "bold")

class MovieApp:

	def __init__(self, root):
		self.mongo = DataService.Mongo("imdb")
		self.mainframe = tk.Frame(root)
		self.root = root
		self.lastRecommend = [] # Last set of recommended titles, used for restoring the recommendation screen
		self.historyList = ()   # Used for the movie history search screen. Stores the user's current movie history.
		self.historyVar = tk.StringVar(value=self.historyList)
		self.searchResults = ()
		self.searchVar = tk.StringVar(value=self.searchResults)
		self.searchTimer = 0
		self.lastSearchContent = "-"
		self.lastSearchUpdate = "-"
		self.backFunction = self.username_screen
		self.username_screen()

	def process_twitter_recommendation(self, username):
		#[TODO] Test list
		recommendations = list()
		recommendations.append("Toy Story (1995)")
		recommendations.append("Harry Potter and the Sorcerer's Stone (2001)")
		recommendations.append("My Neighbor Totoro (1988)")
		recommendations.append("The Matrix (1999)")
		recommendations.append("Star Wars (1977)")
		self.recommendations_screen(recommendations)

	def process_movie_history_recommendation(self):
		#[TODO] Test list
		recommendations = list()
		recommendations.append("Toy Story (1995)")
		recommendations.append("Harry Potter and the Sorcerer's Stone (2001)")
		recommendations.append("My Neighbor Totoro (1988)")
		recommendations.append("The Matrix (1999)")
		recommendations.append("Star Wars (1977)")
		self.recommendations_screen(recommendations)		


	def username_screen(self):
		self.reset_screen()
		self.backFunction = self.username_screen

		centerframe = tk.Frame(self.mainframe)
		centerframe.place(anchor='c', relx=.5, rely=.5)
		centerframe.configure(background=BGCOLOR)

		# Enter Twitter Username
		entryframe = tk.Frame(centerframe)
		entryframe.configure(background=BGCOLOR)
		entryframe.pack()

		label = tk.Label(entryframe, text="Twitter Username: ")
		label.configure(background=BGCOLOR)
		label.pack(side=tk.LEFT)

		self.usernameEntry = tk.Entry(entryframe)
		self.usernameEntry.pack(side=tk.RIGHT)

		# Twitter Recommend Button
		button = tk.Button(centerframe, text="Recommend!", command=partial(self.process_twitter_recommendation, self.usernameEntry.get()))
		button.pack()

		# Movie History Button
		hbutton = tk.Button(centerframe, text="Movie History", command=self.movie_search_screen)
		hbutton.pack()


	def movie_synopsis_screen(self, title):
		self.reset_screen()
		WIDTH = 300
		HEIGHT = 400
		PAD = 10

		# Movie Information
		sideframe = tk.Frame(self.mainframe, width=WINWIDTH-WIDTH-100, height=HEIGHT, padx=PAD)
		sideframe.configure(background=BGCOLOR)
		sideframe.place(anchor='w', relx=0, rely=.65)

		synopframe = tk.Frame(sideframe, pady=30)
		synopframe.configure(background=BGCOLOR)
		synopframe.place(anchor='c', relx=.5, rely=.2)

		titleLabel = tk.Label(synopframe, text=self.simpleTitle(title), justify=tk.LEFT, font=FONT_H1, wraplength=WINWIDTH-WIDTH-PAD*2-100)
		titleLabel.configure(background=BGCOLOR)
		titleLabel.grid(row=0, column=0, sticky="W")

		ratingLabelVar = tk.StringVar()
		ratingLabel = tk.Label(synopframe, textvariable=ratingLabelVar, justify=tk.LEFT, font=FONT_H2)
		ratingLabel.configure(background=BGCOLOR, foreground=TWICOLOR)
		ratingLabel.grid(row=1, column=0, sticky="W")

		yearLabelVar = tk.StringVar()
		yearLabel = tk.Label(synopframe, textvariable=yearLabelVar, justify=tk.LEFT, font=FONT_H2)
		yearLabel.configure(background=BGCOLOR)
		yearLabel.grid(row=2, column=0, sticky="W")

		descLabelVar = tk.StringVar()
		descLabel = tk.Label(synopframe, textvariable=descLabelVar, justify=tk.LEFT, wraplength=WINWIDTH-WIDTH-130)
		descLabel.configure(background=BGCOLOR)
		descLabel.grid(row=3, column=0, sticky="W")

		thr = threading.Thread(target=self.movie_synopsis_information, args=(title, ratingLabelVar, yearLabelVar, descLabelVar))
		thr.start()

		# Back Button
		button = tk.Button(sideframe, text="Go Back", command=self.movie_synopis_exit)
		button.place(anchor='c', relx=.5, rely=.9)

		# Cover Art
		image = Image.open("img/loading.jpg")
		resized = image.resize((WIDTH, HEIGHT), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		coverart = tk.Label(self.mainframe, image=art, borderwidth=5)
		coverart.image = art
		coverart.configure(background='black')
		coverart.place(anchor='e', relx=0.95, rely=.6)

		thr = threading.Thread(target=self.download_coverart, args=(coverart, title, WIDTH, HEIGHT))
		thr.start()


	def movie_synopsis_information(self, title, ratingLabelVar, yearLabelVar, descLabelVar):
		response = self.omdbResponse(title)
		result = json.loads(response.text)

		ratingText = ""
		if "imdbRating" in result:
			rating = round(float(result["imdbRating"]))
			for _ in range(rating):
				ratingText += "★"
			for _ in range(10-rating):
				ratingText += "☆"
		ratingLabelVar.set(ratingText)

		yearText = ""
		if "Year" in result:
			yearText += result["Year"]
		if "Rated" in result:
			yearText += " ("+result["Rated"]+")"
		yearLabelVar.set(yearText)

		if "Plot" in result:
			descLabelVar.set(result["Plot"])

	def movie_synopis_exit(self):
		self.recommendations_screen(self.lastRecommend)


	def movie_search_screen(self):
		self.reset_screen()
		WIDTH = 300
		HEIGHT = 400
		PAD = 10
		self.backFunction = self.movie_search_screen

		# Search Area
		searchframe = tk.Frame(self.mainframe, width=WINWIDTH-WIDTH-100, height=HEIGHT, padx=PAD)
		searchframe.configure(background=BGCOLOR)
		searchframe.place(anchor='w', relx=0.05, rely=.645)

		label = tk.Label(searchframe, text="Movie Search:")
		label.configure(background=BGCOLOR)
		label.grid(row=0, column=0, sticky='W')

		self.searchBar = tk.Entry(searchframe)
		self.searchBar.grid(row=1, column=0, sticky=('E','W'), pady=(0,15))

		searchResults = tk.Listbox(searchframe, listvariable=self.searchVar, height=13, width=55)
		searchResults.grid(row=2, column=0, sticky=('E', 'W'))

		addButton = tk.Button(searchframe, text="Add Movie", command=self.add_to_history)
		addButton.grid(row=3, column=0, sticky=('E','W'), pady=(0,35))

		recommendButton = tk.Button(searchframe, text="Recommend!", command=self.process_movie_history_recommendation)
		recommendButton.grid(row=4, column=0, sticky=('E','W'))

		returnButton = tk.Button(searchframe, text="Go Back", command=self.username_screen)
		returnButton.grid(row=5, column=0, sticky=('E','W'))

		# Movie History List
		sideframe = tk.Frame(self.mainframe, width=WIDTH, height=HEIGHT, padx=PAD)
		sideframe.configure(background=BGCOLOR)
		sideframe.place(anchor='e', relx=1, rely=.58)

		historyBox = tk.Listbox(sideframe, listvariable=self.historyVar, height=25, width=55)
		historyBox.grid(row=0, column=0, sticky=('N','S', 'E', 'W'))

		scrollHistory = tk.Scrollbar(sideframe, orient=tk.VERTICAL, command=historyBox.yview)
		scrollHistory.grid(row=0, column=1, sticky=('N','S'))
		historyBox['yscrollcommand'] = scrollHistory.set

		saveButton = tk.Button(sideframe, text="Save...", command=self.save_history)
		saveButton.grid(row=1, column=0, columnspan=2, sticky=('E','W'))

		loadButton = tk.Button(sideframe, text="Load...", command=self.load_history)
		loadButton.grid(row=2, column=0, columnspan=2, sticky=('E','W'))

	def add_to_history(self):
		print("todo")

	def save_history(self):
		print("todo")

	def load_history(self):
		print("todo")

	def recommendations_screen(self, titles):
		self.reset_screen()
		self.lastRecommend = titles

		centerframe = tk.Frame(self.mainframe)
		centerframe.place(anchor='c', relx=.5, rely=.55)
		centerframe.configure(background=BGCOLOR)

		# Header
		label = tk.Label(centerframe, text="Our Recommendations:", font=FONT_H1)
		label.configure(background=BGCOLOR)
		label.pack()

		# List of Movies
		moviesframe = tk.Frame(centerframe)
		moviesframe.configure(background=BGCOLOR)
		for title in titles:
			self.movie_frame(moviesframe, title, len(titles))
		moviesframe.pack()

		# Back Button
		button = tk.Button(self.mainframe, text="Go Back", command=self.backFunction)
		button.place(anchor='c', relx=.5, rely=.9)


	def movie_frame(self, frame, title, numTitles):
		WIDTH = int(700/numTitles)
		HEIGHT = 250
		movieframe = tk.Frame(frame, width=WIDTH, height=HEIGHT, padx=5)
		movieframe.configure(background=BGCOLOR)
		movieframe.pack(side=tk.LEFT)
		movieframe.pack_propagate(False)

		# Temporary Coverart
		image = Image.open("img/loading.jpg")
		resized = image.resize((WIDTH, HEIGHT-50), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		coverart = tk.Button(movieframe, image=art, command=partial(self.movie_synopsis_screen, title), border=0)
		coverart.image = art
		coverart.configure(background=BGCOLOR)
		coverart.pack(side=tk.TOP)
		coverart.pack_propagate(False)

		# Movie Title
		label = tk.Label(movieframe, text=title, wraplength=WIDTH)
		label.configure(background=BGCOLOR)
		label.pack(side=tk.BOTTOM)

		# Update temporary coverart when finished downloading from OMDB
		thr = threading.Thread(target=self.download_coverart, args=(coverart, title, WIDTH, HEIGHT-50))
		thr.start()


	# Main update loop for timing and such
	def update(self):
		if self.searchBar != None:
			if self.searchBar.get() == self.lastSearchContent:
				self.searchTimer = 0
			elif self.searchBar.get() != self.lastSearchUpdate:
				self.searchTimer = 0
				self.lastSearchUpdate = self.searchBar.get() 
			elif self.searchTimer < AUTOCOMPLETE_DELAY:
				self.searchTimer += 1
			else:
				self.lastSearchContent = self.searchBar.get()
				self.searchResults = ()
				results = self.mongo.db["movies"].find({
						"title":{"$regex":"^"+self.searchBar.get(), "$options":"i"}
					}).limit(15).sort("imdbtitle", 1)
				for result in results:
					self.searchResults = self.searchResults + (result["imdbtitle"],)
				self.searchVar.set(self.searchResults)

		self.root.after(UPDATE_FREQUENCY, self.update)


	def download_coverart(self, label, title, width, height):
		response = -1
		foundImage = False
		localpath = "img/"+self.safeFileString(self.simpleTitle(title))+".jpg"

		if os.path.isfile(localpath):
			foundImage = True #Cover art exists locally (we've downloaded it on a previous run). Use the local copy.

		if not foundImage:
			# New cover art we haven't encountered before. Try to download it, if it exists.
			response = self.omdbResponse(title)
			result = json.loads(response.text)
			if "Poster" in result:
				coverart = urllib.request.urlretrieve(result["Poster"], localpath)
				foundImage = True

		if not foundImage:
			localpath = "img/blank.jpg"
			foundImage = True

		# If cover art found, set it as the new image for the label.
		if foundImage:
			image = Image.open(localpath)
			resized = image.resize((width, height), Image.ANTIALIAS)
			art = ImageTk.PhotoImage(resized)
			label.configure(image=art)
			label.image=art

	def omdbResponse(self, title):
		if "(" in title:
			titleYear = title[title.index("(")+1:title.index(")")]
			return requests.get("http://www.omdbapi.com/?t="+self.simpleTitle(title).replace(" ","+")+"&y="+titleYear+"&r=json")
		else:
			return requests.get("http://www.omdbapi.com/?t="+self.simpleTitle(title).replace(" ","+")+"&r=json")		

	def safeFileString(self, filename):
		keepcharacters = (' ','.','_')
		return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()

	def simpleTitle(self, title):
		if "(" in title:
			return title[:title.index("(")-1]
		return title

	def reset_screen(self):
		self.mainframe.destroy()
		self.mainframe = tk.Frame(root)
		self.mainframe.pack(fill="both", expand=True)
		self.mainframe.configure(background=BGCOLOR)

		logo = tk.PhotoImage(file="img/logo.gif")
		self.logo = tk.Label(self.mainframe, image=logo)
		self.logo.image = logo
		self.logo.configure(background=BGCOLOR)
		self.logo.pack()

		self.searchBar = None



root = tk.Tk()
root.configure(background=BGCOLOR)
root.title("Twitter Movie Recommendations")
root.minsize(width=WINWIDTH, height=WINHEIGHT)
root.maxsize(width=WINWIDTH, height=WINHEIGHT)

app = MovieApp(root)
root.after(UPDATE_FREQUENCY, app.update)

while True:
	root.update_idletasks()
	root.update()
	