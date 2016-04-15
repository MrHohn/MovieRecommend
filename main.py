import json
import os.path
import requests
import threading
import urllib.request
import tkinter as tk
from PIL import Image, ImageTk

BGCOLOR = 'white'

class MovieApp:

	def __init__(self, root):
		self.mainframe = tk.Frame(root)
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

	def username_screen(self):
		self.reset_screen()

		centerframe = tk.Frame(self.mainframe)
		centerframe.place(anchor='c', relx=.5, rely=.5)
		centerframe.configure(background=BGCOLOR)

		entryframe = tk.Frame(centerframe)
		entryframe.configure(background=BGCOLOR)
		entryframe.pack()

		label = tk.Label(entryframe, text="Twitter Username: ")
		label.configure(background=BGCOLOR)
		label.pack(side=tk.LEFT)

		username = tk.Entry(entryframe)
		username.pack(side=tk.RIGHT)

		button = tk.Button(centerframe, text="Recommend!", command=self.process_recommendation)
		button.pack()

	def recommendations_screen(self, titles):
		self.reset_screen()

		centerframe = tk.Frame(self.mainframe)
		centerframe.place(anchor='c', relx=.5, rely=.55)
		centerframe.configure(background=BGCOLOR)

		label = tk.Label(centerframe, text="Our Recommendations:")
		label.configure(background=BGCOLOR)
		label.pack()

		moviesframe = tk.Frame(centerframe)
		moviesframe.configure(background=BGCOLOR)
		for title in titles:
			self.movie_frame(moviesframe, title, len(titles))
		moviesframe.pack()

		button = tk.Button(centerframe, text="Go Back", command=self.username_screen)
		button.pack(side=tk.BOTTOM)

	def movie_frame(self, frame, title, numTitles):
		WIDTH = int(700/numTitles)
		HEIGHT = 250
		movieframe = tk.Frame(frame, width=WIDTH, height=HEIGHT)
		movieframe.configure(background=BGCOLOR)
		movieframe.pack(side=tk.LEFT)
		movieframe.pack_propagate(False)

		image = Image.open("img/loading.jpg")
		resized = image.resize((WIDTH, HEIGHT-50), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		coverart = tk.Label(movieframe, image=art)
		coverart.image = art
		coverart.configure(background=BGCOLOR)
		coverart.pack(side=tk.TOP)
		coverart.pack_propagate(False)

		label = tk.Label(movieframe, text=title, wraplength=WIDTH)
		label.configure(background=BGCOLOR)
		label.pack(side=tk.BOTTOM)

		thr = threading.Thread(target=self.download_coverart, args=(coverart, title, WIDTH, HEIGHT-50))
		thr.start()

	def download_coverart(self, label, title, width, height):
		response = -1
		foundImage = False
		titleName = title
		if "(" in title:
			titleName = title[:title.index("(")-1]
		localpath = "img/"+self.safeFileString(titleName)+".jpg"

		if os.path.isfile(localpath):
			foundImage = True #Cover art exists locally (we've downloaded it on a previous run). Use the local copy.

		if not foundImage:
			# New cover art we haven't encountered before. Try to download it, if it exists.
			if "(" in title:
				titleYear = title[title.index("(")+1:title.index(")")]
				response = requests.get("http://www.omdbapi.com/?t="+titleName.replace(" ","+")+"&y="+titleYear+"&r=json")
			else:
				response = requests.get("http://www.omdbapi.com/?t="+titleName.replace(" ","+")+"&r=json")

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

	def safeFileString(self, filename):
		keepcharacters = (' ','.','_')
		return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


root = tk.Tk()
root.configure(background=BGCOLOR)
root.title("Twitter Movie Recommendations")
root.minsize(width=800, height=600)
root.maxsize(width=800, height=600)

app = MovieApp(root)

root.mainloop()
root.destroy()