import json
import os.path
import requests
import threading
import urllib.request
import tkinter as tk
from PIL import Image, ImageTk
from functools import partial

BGCOLOR = 'white'
TWICOLOR = '#4099FF'
WINWIDTH = 800
WINHEIGHT = 600

FONT_H1 = ("Helvetica", 18, "bold")
FONT_H2 = ("Helvetica", 12, "bold")

class MovieApp:

	def __init__(self, root):
		self.mainframe = tk.Frame(root)
		self.lastRecommend = [] #Last set of recommended titles, used for restoring the recommendation screen
		self.username_screen()

	def process_recommendation(self):
		print("todo")
		#Test list
		recommendations = list()
		recommendations.append("Toy Story (1995)")
		recommendations.append("Harry Potter and the Sorcerer's Stone (2001)")
		recommendations.append("My Neighbor Totoro (1988)")
		recommendations.append("The Matrix (1999)")
		recommendations.append("Star Wars (1977)")
		self.recommendations_screen(recommendations)


	def username_screen(self):
		self.reset_screen()

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

		username = tk.Entry(entryframe)
		username.pack(side=tk.RIGHT)

		# Twitter Recommend Button
		button = tk.Button(centerframe, text="Recommend!", command=self.process_recommendation)
		button.pack()


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
		button = tk.Button(self.mainframe, text="Go Back", command=self.username_screen)
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



root = tk.Tk()
root.configure(background=BGCOLOR)
root.title("Twitter Movie Recommendations")
root.minsize(width=WINWIDTH, height=WINHEIGHT)
root.maxsize(width=WINWIDTH, height=WINHEIGHT)

app = MovieApp(root)

root.mainloop()