package main

import "time"

var data = []Series{
	{
		Title: "IT Crowd",
		Info: "" +
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by " +
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
		ReleaseDate: date("2006-02-03"),
		Seasons: []Season{
			{
				Title:      "Season 1",
				FirstAired: date("2006-02-03"),
				LastAired:  date("2006-03-03"),
				Episodes: []Episode{
					{
						Title:   "Yesterday's Jam",
						AirDate: date("2006-02-03"),
					},
					{
						Title:   "Calamity Jen",
						AirDate: date("2006-02-03"),
					},
					{
						Title:   "Fifty-Fifty",
						AirDate: date("2006-02-10"),
					},
					{
						Title:   "The Red Door",
						AirDate: date("2006-02-17"),
					},
					{
						Title:   "The Haunting of Bill Crouse",
						AirDate: date("2006-02-24"),
					},
					{
						Title:   "Aunt Irma Visits",
						AirDate: date("2006-03-03"),
					},
				},
			},
			{
				Title:      "Season 2",
				FirstAired: date("2007-08-24"),
				LastAired:  date("2007-09-28"),
				Episodes: []Episode{
					{
						Title:   "The Work Outing",
						AirDate: date("2006-08-24"),
					},
					{
						Title:   "Return of the Golden Child",
						AirDate: date("2007-08-31"),
					},
					{
						Title:   "Moss and the German",
						AirDate: date("2007-09-07"),
					},
					{
						Title:   "The Dinner Party",
						AirDate: date("2007-09-14"),
					},
					{
						Title:   "Smoke and Mirrors",
						AirDate: date("2007-09-21"),
					},
					{
						Title:   "Men Without Women",
						AirDate: date("2007-09-28"),
					},
				},
			},
			{
				Title:      "Season 3",
				FirstAired: date("2008-11-21"),
				LastAired:  date("2008-12-26"),
				Episodes: []Episode{
					{
						Title:   "From Hell",
						AirDate: date("2008-11-21"),
					},
					{
						Title:   "Are We Not Men?",
						AirDate: date("2008-11-28"),
					},
					{
						Title:   "Tramps Like Us",
						AirDate: date("2008-12-05"),
					},
					{
						Title:   "The Speech",
						AirDate: date("2008-12-12"),
					},
					{
						Title:   "Friendface",
						AirDate: date("2008-12-19"),
					},
					{
						Title:   "Calendar Geeks",
						AirDate: date("2008-12-26"),
					},
				},
			},
			{
				Title:      "Season 4",
				FirstAired: date("2010-06-25"),
				LastAired:  date("2010-07-30"),
				Episodes: []Episode{
					{
						Title:   "Jen The Fredo",
						AirDate: date("2010-06-25"),
					},
					{
						Title:   "The Final Countdown",
						AirDate: date("2010-07-02"),
					},
					{
						Title:   "Something Happened",
						AirDate: date("2010-07-09"),
					},
					{
						Title:   "Italian For Beginners",
						AirDate: date("2010-07-16"),
					},
					{
						Title:   "Bad Boys",
						AirDate: date("2010-07-23"),
					},
					{
						Title:   "Reynholm vs Reynholm",
						AirDate: date("2010-07-30"),
					},
				},
			},
		},
	},
	{
		Title: "Silicon Valley",
		Info: "" +
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and " +
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
		ReleaseDate: date("2014-04-06"),
		Seasons: []Season{
			{
				Title:      "Season 1",
				FirstAired: date("2006-02-03"),
				LastAired:  date("2006-03-03"),
				Episodes: []Episode{
					{
						Title:   "Minimum Viable Product",
						AirDate: date("2014-04-06"),
					},
					{
						Title:   "The Cap Table",
						AirDate: date("2014-04-13"),
					},
					{
						Title:   "Articles of Incorporation",
						AirDate: date("2014-04-20"),
					},
					{
						Title:   "Fiduciary Duties",
						AirDate: date("2014-04-27"),
					},
					{
						Title:   "Signaling Risk",
						AirDate: date("2014-05-04"),
					},
					{
						Title:   "Third Party Insourcing",
						AirDate: date("2014-05-11"),
					},
					{
						Title:   "Proof of Concept",
						AirDate: date("2014-05-18"),
					},
					{
						Title:   "Optimal Tip-to-Tip Efficiency",
						AirDate: date("2014-06-01"),
					},
				},
			},
			{
				Title:      "Season 2",
				FirstAired: date("2007-08-24"),
				LastAired:  date("2007-09-28"),
				Episodes: []Episode{
					{
						Title:   "Sand Hill Shuffle",
						AirDate: date("2015-04-12"),
					},
					{
						Title:   "Runaway Devaluation",
						AirDate: date("2015-04-19"),
					},
					{
						Title:   "Bad Money",
						AirDate: date("2015-04-26"),
					},
					{
						Title:   "The Lady",
						AirDate: date("2015-05-03"),
					},
					{
						Title:   "Server Space",
						AirDate: date("2015-05-10"),
					},
					{
						Title:   "Homicide",
						AirDate: date("2015-05-17"),
					},
					{
						Title:   "Adult Content",
						AirDate: date("2015-05-24"),
					},
					{
						Title:   "White Hat/Black Hat",
						AirDate: date("2015-05-31"),
					},
					{
						Title:   "Binding Arbitration",
						AirDate: date("2015-06-07"),
					},
					{
						Title:   "Two Days of the Condor",
						AirDate: date("2015-06-14"),
					},
				},
			},
			{
				Title:      "Season 3",
				FirstAired: date("2008-11-21"),
				LastAired:  date("2008-12-26"),
				Episodes: []Episode{
					{
						Title:   "Founder Friendly",
						AirDate: date("2016-04-24"),
					},
					{
						Title:   "Two in the Box",
						AirDate: date("2016-05-01"),
					},
					{
						Title:   "Meinertzhagen's Haversack",
						AirDate: date("2016-05-08"),
					},
					{
						Title:   "Maleant Data Systems Solutions",
						AirDate: date("2016-05-15"),
					},
					{
						Title:   "The Empty Chair",
						AirDate: date("2016-05-22"),
					},
					{
						Title:   "Bachmanity Insanity",
						AirDate: date("2016-05-29"),
					},
					{
						Title:   "To Build a Better Beta",
						AirDate: date("2016-06-05"),
					},
					{
						Title:   "Bachman's Earnings Over-Ride",
						AirDate: date("2016-06-12"),
					},
					{
						Title:   "Daily Active Users",
						AirDate: date("2016-06-19"),
					},
					{
						Title:   "The Uptick",
						AirDate: date("2016-06-26"),
					},
				},
			},
			{
				Title:      "Season 4",
				FirstAired: date("2010-06-25"),
				LastAired:  date("2010-07-30"),
				Episodes: []Episode{
					{
						Title:   "Success Failure",
						AirDate: date("2017-04-23"),
					},
					{
						Title:   "Terms of Service",
						AirDate: date("2017-04-30"),
					},
					{
						Title:   "Intellectual Property",
						AirDate: date("2017-05-07"),
					},
					{
						Title:   "Teambuilding Exercise",
						AirDate: date("2017-05-14"),
					},
					{
						Title:   "The Blood Boy",
						AirDate: date("2017-05-21"),
					},
					{
						Title:   "Customer Service",
						AirDate: date("2017-05-28"),
					},
					{
						Title:   "The Patent Troll",
						AirDate: date("2017-06-04"),
					},
					{
						Title:   "The Keenan Vortex",
						AirDate: date("2017-06-11"),
					},
					{
						Title:   "Hooli-Con",
						AirDate: date("2017-06-18"),
					},
					{
						Title:   "Server Error",
						AirDate: date("2017-06-25"),
					},
				},
			},
			{
				Title:      "Season 5",
				FirstAired: date("2018-03-25"),
				LastAired:  date("2018-05-13"),
				Episodes: []Episode{
					{
						Title:   "Grow Fast or Die Slow",
						AirDate: date("2018-03-25"),
					},
					{
						Title:   "Reorientation",
						AirDate: date("2018-04-01"),
					},
					{
						Title:   "Chief Operating Officer",
						AirDate: date("2018-04-08"),
					},
					{
						Title:   "Tech Evangelist",
						AirDate: date("2018-04-15"),
					},
					{
						Title:   "Facial Recognition",
						AirDate: date("2018-04-22"),
					},
					{
						Title:   "Artificial Emotional Intelligence",
						AirDate: date("2018-04-29"),
					},
					{
						Title:   "Initial Coin Offering",
						AirDate: date("2018-05-06"),
					},
					{
						Title:   "Fifty-One Percent",
						AirDate: date("2018-05-13"),
					},
				},
			},
		},
	},
}

const dateISO8601 = "2006-01-02"

func date(date string) time.Time {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
