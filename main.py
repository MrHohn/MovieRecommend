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
		recommendations.append("Tonari no Totoro (1988)")
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
		movieframe = tk.Frame(frame, width=int(700/numTitles), height=250)
		movieframe.configure(background=BGCOLOR)
		movieframe.pack(side=tk.LEFT)
		movieframe.pack_propagate(False)

		image = Image.open("img/blank.jpg")
		resized = image.resize((int(700/numTitles), 200), Image.ANTIALIAS)
		art = ImageTk.PhotoImage(resized)
		coverart = tk.Label(movieframe, image=art)
		coverart.image = art
		coverart.configure(background=BGCOLOR)
		coverart.pack(side=tk.TOP)
		coverart.pack_propagate(False)

		label = tk.Label(movieframe, text=title, wraplength=int(700/numTitles))
		label.configure(background=BGCOLOR)
		label.pack(side=tk.BOTTOM)




root = tk.Tk()
root.configure(background=BGCOLOR)
root.title("Twitter Movie Recommendations")
root.minsize(width=800, height=600)
root.maxsize(width=800, height=600)

app = MovieApp(root)

root.mainloop()
root.destroy()