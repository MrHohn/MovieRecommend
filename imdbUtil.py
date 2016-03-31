from datetime import date
import re

#Cases:
#	Single year (2003)
#	Range (2003-2010) -> return earliest year in range
#	Unknown (non-numerical value) -> In IMDB, "????" is used to mean "currently airing/no release date announced yet"... return current year
def parseYear(year):
	if "-" in year:
		return year[:year.index("-")]
	try:
		int(year)
		return year
	except ValueError:
		return date.today().year

#IMDB Title Format:
#	TITLE (YEAR) {EPISODE}
#	If episode included, then it is a TV show, not a movie.
def isShow(title):
	if "{" in title:
		return True
	return False

#Strips TV episode from IMDB title
def stripEpisode(title):
	return re.sub(r"\s{.*}", "", title)

#Formats IMDB title as it should appear in the database
def formatTitle(title):
	return title.encode('ascii', 'ignore').decode('ascii').strip()