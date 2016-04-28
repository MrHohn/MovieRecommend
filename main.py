import json
import os.path
import requests
import threading
import urllib.request
import tkinter as tk
import DataService
import imdbUtil
from PIL import Image, ImageTk
from functools import partial
from tkinter import filedialog
from movieRecommend import MovieRecommend

BGCOLOR = 'white'
TWICOLOR = '#4099FF'
WINWIDTH = 800
WINHEIGHT = 600
AUTOCOMPLETE_DELAY = 5
UPDATE_FREQUENCY = 100 #milliseconds

MOVIE_MODE = 0
TAG_MODE = 1

FONT_H1 = ("Helvetica", 18, "bold")
FONT_H2 = ("Helvetica", 12, "bold")

class MovieApp:

	def __init__(self, root):
		self.mongo = DataService.Mongo("imdb")
		self.mainframe = tk.Frame(root)
		self.root = root
		self.lastRecommend = [] # Last set of recommended titles, used for restoring the recommendation screen
		self.searchMode = MOVIE_MODE
		self.historyList = [0,0]
		self.historyList[MOVIE_MODE] = set()
		self.historyList[TAG_MODE] = set()
		self.historyVar = tk.StringVar(value=tuple(self.historyList[self.searchMode]))
		self.searchResults = set()
		self.searchVar = tk.StringVar(value=tuple(self.searchResults))
		self.searchTimer = 0
		self.lastSearchContent = "-"
		self.lastSearchUpdate = "-"
		self.backFunction = self.username_screen
		self.username_screen()

	def process_twitter_recommendation(self, usernameWidget):
		username = usernameWidget.get()
		print(username)

		recommendations = []
		recommender = MovieRecommend(self.mongo)
		all_recommendations = recommender.recommend_movies_for_twitter_integrated(username)
		total = min(5, len(all_recommendations))
		for i in range(total):
			recommendations.append(all_recommendations[i])
		self.recommendations_screen(recommendations)

	def process_movie_history_recommendation(self):
		print(self.historyList[MOVIE_MODE])

		recommendations = []
		recommender = MovieRecommend(self.mongo)
		all_recommendations = recommender.recommend_movies_based_on_history(self.historyList[MOVIE_MODE])
		all_recommendations = recommender.get_titles_by_mids(all_recommendations)
		total = min(5, len(all_recommendations))
		for i in range(total):
			recommendations.append(all_recommendations[i])
		self.recommendations_screen(recommendations)

	def process_tag_recommendation(self):
		print(self.historyList[TAG_MODE])

		recommendations = []
		recommender = MovieRecommend(self.mongo)
		all_recommendations = recommender.recommend_movies_based_on_tags_integrated(self.historyList[TAG_MODE])
		total = min(5, len(all_recommendations))
		for i in range(total):
			recommendations.append(all_recommendations[i])
		self.recommendations_screen(recommendations)


	#
	# ========================================= Title Screen ===========================================
	#
	def username_screen(self):
		self.reset_screen()
		self.backFunction = self.username_screen

		self.lastSearchContent += "-"
		self.lastSearchUpdate += "-"
		self.searchResults = set()
		self.searchVar.set(tuple(self.searchResults))

		PAD = 50

		centerframe = tk.Frame(self.mainframe)
		centerframe.place(anchor='c', relx=.5, rely=.6)
		centerframe.configure(background=BGCOLOR)

		label = tk.Label(self.mainframe, text="Recommendation Mode:", font=FONT_H1, pady=32)
		label.configure(background=BGCOLOR)
		label.place(anchor='c', relx=.5, rely=.3)

		label = tk.Label(centerframe, text="Twitter Account", font=FONT_H2)
		label.configure(background=BGCOLOR)
		label.grid(row=0, column=0, padx=(0,PAD))

		label = tk.Label(centerframe, text="Watch History", font=FONT_H2)
		label.configure(background=BGCOLOR)
		label.grid(row=0, column=1, padx=(0,PAD))

		label = tk.Label(centerframe, text="Tags/Genres", font=FONT_H2)
		label.configure(background=BGCOLOR)
		label.grid(row=0, column=2)

		label = tk.Label(centerframe, text="Username:")
		label.configure(background=BGCOLOR)
		label.grid(row=2, column=0, pady=(20,0), padx=(0,PAD))

		self.usernameEntry = tk.Entry(centerframe)
		self.usernameEntry.grid(row=3, column=0, padx=(0,PAD))

		# Twitter Recommend Button
		image = Image.open("img/twitterIcon.gif")
		resized = image.resize((150, 150), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		button = tk.Button(centerframe, image=art, border=4, command=partial(self.process_twitter_recommendation, self.usernameEntry))
		button.image = art
		button.grid(row=1, column=0, padx=(0,PAD))

		# Movie History Button
		image = Image.open("img/historyIcon.gif")
		resized = image.resize((150, 150), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		hbutton = tk.Button(centerframe, image=art, border=4, command=self.init_movie_search)
		hbutton.image = art
		hbutton.grid(row=1, column=1, padx=(0,PAD))

		# Tag Search Button
		image = Image.open("img/tagIcon.gif")
		resized = image.resize((150, 150), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		tbutton = tk.Button(centerframe, image=art, border=4, command=self.init_tag_search)
		tbutton.image = art
		tbutton.grid(row=1, column=2)

	def init_movie_search(self):
		self.searchMode = MOVIE_MODE
		self.search_screen()

	def init_tag_search(self):
		self.searchMode = TAG_MODE
		self.search_screen()


	#
	# ========================================= Movie Synopsis Screen ===========================================
	#
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

		titleLabel = tk.Label(synopframe, text=imdbUtil.simpleTitle(title), justify=tk.LEFT, font=FONT_H1, wraplength=WINWIDTH-WIDTH-PAD*2-100)
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
			if result["imdbRating"] == "N/A":
				rating = 0
			else:
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


	#
	# ========================================= Search and History Screen ===========================================
	#
	def search_screen(self):
		self.reset_screen()
		WIDTH = 300
		HEIGHT = 400
		PAD = 10
		self.backFunction = self.search_screen

		objectName = ""
		recommendFunction = None
		if self.searchMode == MOVIE_MODE:
			objectName = "Movie"
			recommendFunction = self.process_movie_history_recommendation
		elif self.searchMode == TAG_MODE:
			objectName = "Tag"
			recommendFunction = self.process_tag_recommendation

		# Search Area
		searchframe = tk.Frame(self.mainframe, width=WINWIDTH-WIDTH-100, height=HEIGHT, padx=PAD)
		searchframe.configure(background=BGCOLOR)
		searchframe.place(anchor='w', relx=0.05, rely=.645)

		label = tk.Label(searchframe, text=objectName+" Search:")
		label.configure(background=BGCOLOR)
		label.grid(row=0, column=0, sticky='W')

		self.searchBar = tk.Entry(searchframe)
		self.searchBar.grid(row=1, column=0, sticky=('E','W'), pady=(0,15), columnspan=2)

		self.searchList = tk.Listbox(searchframe, listvariable=self.searchVar, height=13, width=55, selectmode=tk.EXTENDED)
		self.searchList.grid(row=2, column=0, sticky=('N','S', 'E', 'W'))

		scrollSearch = tk.Scrollbar(searchframe, orient=tk.VERTICAL, command=self.searchList.yview)
		scrollSearch.grid(row=2, column=1, sticky=('N','S'))
		self.searchList['yscrollcommand'] = scrollSearch.set

		addButton = tk.Button(searchframe, text="Add "+objectName, command=self.add_to_history)
		addButton.grid(row=3, column=0, sticky=('E','W'), pady=(0,35), columnspan=2)

		recommendButton = tk.Button(searchframe, text="Recommend!", command=recommendFunction)
		recommendButton.grid(row=4, column=0, sticky=('E','W'), columnspan=2)

		returnButton = tk.Button(searchframe, text="Go Back", command=self.username_screen)
		returnButton.grid(row=5, column=0, sticky=('E','W'), columnspan=2)

		# Movie History List
		sideframe = tk.Frame(self.mainframe, width=WIDTH, height=HEIGHT, padx=PAD)
		sideframe.configure(background=BGCOLOR)
		sideframe.place(anchor='e', relx=1, rely=.58)

		self.historyVar.set(tuple(self.historyList[self.searchMode]))
		self.historyBox = tk.Listbox(sideframe, listvariable=self.historyVar, height=25, width=55, selectmode=tk.EXTENDED)
		self.historyBox.bind("<Delete>", self.delete_from_history)
		self.historyBox.grid(row=0, column=0, sticky=('N','S', 'E', 'W'))

		scrollHistory = tk.Scrollbar(sideframe, orient=tk.VERTICAL, command=self.historyBox.yview)
		scrollHistory.grid(row=0, column=1, sticky=('N','S'))
		self.historyBox['yscrollcommand'] = scrollHistory.set

		saveButton = tk.Button(sideframe, text="Save...", command=self.save_history)
		saveButton.grid(row=1, column=0, columnspan=2, sticky=('E','W'))

		loadButton = tk.Button(sideframe, text="Load...", command=self.load_history)
		loadButton.grid(row=2, column=0, columnspan=2, sticky=('E','W'))

	def add_to_history(self):
		selectedMovies = self.searchList.curselection()
		for selectionIndex in selectedMovies:
			self.historyList[self.searchMode].add(self.searchList.get(selectionIndex))
		self.historyVar.set(tuple(self.historyList[self.searchMode]))

	def delete_from_history(self, event):
		selectedMovies = self.historyBox.curselection()
		for selectionIndex in selectedMovies:
			self.historyList[self.searchMode].remove(self.historyBox.get(selectionIndex))
		self.historyVar.set(tuple(self.historyList[self.searchMode]))

	def save_history(self):
		f = filedialog.asksaveasfile(mode='w', filetypes=[self.get_filetype()], defaultextension=self.get_filetype()[1])
		if f is None:
			return
		for movie in self.historyList[self.searchMode]:
			f.write(movie+"\n")
		f.close()

	def load_history(self):
		f = filedialog.askopenfile(mode='r', filetypes=[self.get_filetype()], defaultextension=self.get_filetype()[1])
		if f is None:
			return
		self.historyList[self.searchMode].clear()
		for line in f:
			self.historyList[self.searchMode].add(line)
		self.historyVar.set(tuple(self.historyList[self.searchMode]))
		f.close()

	def get_filetype(self):
		if self.searchMode == MOVIE_MODE:
			return ("Movie List", ".mvl")
		elif self.searchMode == TAG_MODE:
			return ("Tag List", ".tgl")
		return None

	#
	# ========================================= Recommendation Screen ===========================================
	#
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

	# A frame widget containing an image of the movie's cover art, as well as the title of the movie
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


	#
	# ========================================= Other Functions ===========================================
	#

	# Main update loop for timing and such
	def update(self):
		# Handle auto-complete for the search form
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
				self.searchResults = []

				collectionName = ""
				primFieldName = ""
				secFieldName = ""
				matchValue = self.searchBar.get()
				if self.searchMode == MOVIE_MODE:
					# collectionName = "movies"
					# primFieldName = "imdbtitle"
					# secFieldName = "title"
					self.mongo.db = self.mongo.client["integration"]
					collectionName = "copy_movies"
					primFieldName = "imdbtitle"
					secFieldName = "title"
				elif self.searchMode == TAG_MODE:
					# collectionName = "keywords"
					# primFieldName = "keyword"
					# secFieldName = "keyword"
					self.mongo.db = self.mongo.client["integration"]
					collectionName = "normalized_tags"
					primFieldName = "tag"
					secFieldName = "tag"
					matchValue = matchValue.replace("-", "[-\s]").replace(" ", "[-\s]")

				results = self.mongo.db[collectionName].find({
						primFieldName:{"$regex":"^"+matchValue, "$options":"i"}
					}).limit(30).sort(primFieldName, 1)
				for result in results:
					self.searchResults.append(result[primFieldName])
				results = self.mongo.db[collectionName].find({
						secFieldName:{"$regex":matchValue, "$options":"i"}
					}).limit(30).sort(primFieldName, 1)
				for result in results:
					self.searchResults.append(result[primFieldName])
				self.searchResults = self.prefixOrder(self.searchResults, self.searchBar.get())
				self.searchVar.set(tuple(self.searchResults))
				self.searchList.selection_clear(0, tk.END)

		self.root.after(UPDATE_FREQUENCY, self.update)

	# Find and download a coverart image from IMDB for the given movie, and attach the image to a label widget.
	def download_coverart(self, label, title, width, height):
		response = -1
		foundImage = False
		localpath = "img/"+self.safeFileString(imdbUtil.simpleTitle(title))+".jpg"

		if os.path.isfile(localpath):
			foundImage = True #Cover art exists locally (we've downloaded it on a previous run). Use the local copy.

		if not foundImage:
			# New cover art we haven't encountered before. Try to download it, if it exists.
			response = self.omdbResponse(title)
			result = json.loads(response.text)
			print
			if "Poster" in result and "http" in result["Poster"]:
				coverart = urllib.request.urlretrieve(result["Poster"], localpath)
				foundImage = True

		if not foundImage:
			# print("[UI] Poster not found.")
			localpath = "img/blank.jpg"
			foundImage = True

		# If cover art found, set it as the new image for the label.
		if foundImage:
			image = Image.open(localpath)
			resized = image.resize((width, height), Image.ANTIALIAS)
			art = ImageTk.PhotoImage(resized)
			label.configure(image=art)
			label.image=art

	# Get extended IMDB data for a specific movie from OMDB, rather than our local database (cover art, etc).
	def omdbResponse(self, title):
		if "(" in title:
			titleYear = title[title.index("(")+1:title.index(")")]
			return requests.get("http://www.omdbapi.com/?t="+imdbUtil.simpleTitle(title).replace(" ","+")+"&y="+titleYear+"&r=json")
		else:
			return requests.get("http://www.omdbapi.com/?t="+imdbUtil.simpleTitle(title).replace(" ","+")+"&r=json")		

	# Remove characters from a string that could be problematic as part of a file name.
	def safeFileString(self, filename):
		keepcharacters = (' ','.','_')
		return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()

	# Order elements in list by relevance to a prefix.
	# IE: If prefix was "hello":
	#			Strings that start with "hello" are priority 1
	#			Remaining unmatched strings that start with "hell" are priority 2
	#			Remaining unmatched strings that start with "hel" are priority 3
	#			etc...
	def prefixOrder(self, listToSort, prefix):
		sortedElems = sorted(list(set(listToSort)))
		orderedList = []

		for prefixInd in reversed(range(len(prefix)+1)):
			if sortedElems is None:
				break
			remainingElems = list(sortedElems)
			preMatch = prefix[:prefixInd]
			for item in remainingElems:
				if self.cleanForSearch(item).startswith(self.cleanForSearch(preMatch)):
					sortedElems.remove(item)
					orderedList.append(item)

		return orderedList

	def cleanForSearch(self, string):
		lstr = string.lower()
		if lstr.startswith("the "):
			return lstr[4:]
		else:
			return lstr

	# Reset back to a blank screen (containing just the header logo)
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
root.mainloop()
	