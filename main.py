import tkinter as tk

BGCOLOR = 'white'

class MovieApp:

	def __init__(self, root):
		centerframe = tk.Frame(root)
		centerframe.place(anchor='c', relx=.5, rely=.5)
		centerframe.configure(background=BGCOLOR)

		entryframe = tk.Frame(centerframe)
		entryframe.configure(background=BGCOLOR)
		entryframe.pack()

		logo = tk.PhotoImage(file="img/logo.gif")

		self.logo = tk.Label(root, image=logo)
		self.logo.image = logo
		self.logo.configure(background=BGCOLOR)
		self.logo.pack()

		self.label = tk.Label(entryframe, text="Twitter Username: ")
		self.label.configure(background=BGCOLOR)
		self.label.pack(side=tk.LEFT)

		self.username = tk.Entry(entryframe, text="Hello World")
		self.username.pack(side=tk.RIGHT)

		self.button = tk.Button(centerframe, text="Recommend!", command=self.process_recommendation)
		self.button.pack()

	def process_recommendation(self):
		print("todo")

root = tk.Tk()
root.configure(background=BGCOLOR)
root.title("Twitter Movie Recommendations")
root.minsize(width=800, height=600)
root.maxsize(width=800, height=600)

app = MovieApp(root)

root.mainloop()
root.destroy()