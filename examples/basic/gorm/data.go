package main

import "time"

var data = []Series{
	{
		ID:      "",
		Comment: "",
		Title:   "IT Crowd",
		Info: "" +
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by " +
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
		ReleaseDate: date("2006-02-03"),
		Seasons: []Season{
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 1",
				FirstAired: date("2006-02-03"),
				LastAired:  date("2006-03-03"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Yesterday's Jam",
						AirDate:  date("2006-02-03"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Calamity Jen",
						AirDate:  date("2006-02-03"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Fifty-Fifty",
						AirDate:  date("2006-02-10"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Red Door",
						AirDate:  date("2006-02-17"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Haunting of Bill Crouse",
						AirDate:  date("2006-02-24"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Aunt Irma Visits",
						AirDate:  date("2006-03-03"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 2",
				FirstAired: date("2007-08-24"),
				LastAired:  date("2007-09-28"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Work Outing",
						AirDate:  date("2006-08-24"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Return of the Golden Child",
						AirDate:  date("2007-08-31"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Moss and the German",
						AirDate:  date("2007-09-07"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Dinner Party",
						AirDate:  date("2007-09-14"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Smoke and Mirrors",
						AirDate:  date("2007-09-21"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Men Without Women",
						AirDate:  date("2007-09-28"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 3",
				FirstAired: date("2008-11-21"),
				LastAired:  date("2008-12-26"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "From Hell",
						AirDate:  date("2008-11-21"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Are We Not Men?",
						AirDate:  date("2008-11-28"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Tramps Like Us",
						AirDate:  date("2008-12-05"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Speech",
						AirDate:  date("2008-12-12"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Friendface",
						AirDate:  date("2008-12-19"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Calendar Geeks",
						AirDate:  date("2008-12-26"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 4",
				FirstAired: date("2010-06-25"),
				LastAired:  date("2010-07-30"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Jen The Fredo",
						AirDate:  date("2010-06-25"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Final Countdown",
						AirDate:  date("2010-07-02"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Something Happened",
						AirDate:  date("2010-07-09"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Italian For Beginners",
						AirDate:  date("2010-07-16"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Bad Boys",
						AirDate:  date("2010-07-23"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Reynholm vs Reynholm",
						AirDate:  date("2010-07-30"),
					},
				},
			},
		},
	},
	{
		ID:      "",
		Comment: "",
		Title:   "Silicon Valley",
		Info: "" +
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and " +
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
		ReleaseDate: date("2014-04-06"),
		Seasons: []Season{
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 1",
				FirstAired: date("2006-02-03"),
				LastAired:  date("2006-03-03"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Minimum Viable Product",
						AirDate:  date("2014-04-06"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Cap Table",
						AirDate:  date("2014-04-13"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Articles of Incorporation",
						AirDate:  date("2014-04-20"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Fiduciary Duties",
						AirDate:  date("2014-04-27"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Signaling Risk",
						AirDate:  date("2014-05-04"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Third Party Insourcing",
						AirDate:  date("2014-05-11"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Proof of Concept",
						AirDate:  date("2014-05-18"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Optimal Tip-to-Tip Efficiency",
						AirDate:  date("2014-06-01"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 2",
				FirstAired: date("2007-08-24"),
				LastAired:  date("2007-09-28"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Sand Hill Shuffle",
						AirDate:  date("2015-04-12"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Runaway Devaluation",
						AirDate:  date("2015-04-19"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Bad Money",
						AirDate:  date("2015-04-26"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Lady",
						AirDate:  date("2015-05-03"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Server Space",
						AirDate:  date("2015-05-10"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Homicide",
						AirDate:  date("2015-05-17"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Adult Content",
						AirDate:  date("2015-05-24"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "White Hat/Black Hat",
						AirDate:  date("2015-05-31"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Binding Arbitration",
						AirDate:  date("2015-06-07"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Two Days of the Condor",
						AirDate:  date("2015-06-14"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 3",
				FirstAired: date("2008-11-21"),
				LastAired:  date("2008-12-26"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Founder Friendly",
						AirDate:  date("2016-04-24"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Two in the Box",
						AirDate:  date("2016-05-01"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Meinertzhagen's Haversack",
						AirDate:  date("2016-05-08"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Maleant Data Systems Solutions",
						AirDate:  date("2016-05-15"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Empty Chair",
						AirDate:  date("2016-05-22"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Bachmanity Insanity",
						AirDate:  date("2016-05-29"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "To Build a Better Beta",
						AirDate:  date("2016-06-05"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Bachman's Earnings Over-Ride",
						AirDate:  date("2016-06-12"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Daily Active Users",
						AirDate:  date("2016-06-19"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Uptick",
						AirDate:  date("2016-06-26"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 4",
				FirstAired: date("2010-06-25"),
				LastAired:  date("2010-07-30"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Success Failure",
						AirDate:  date("2017-04-23"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Terms of Service",
						AirDate:  date("2017-04-30"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Intellectual Property",
						AirDate:  date("2017-05-07"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Teambuilding Exercise",
						AirDate:  date("2017-05-14"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Blood Boy",
						AirDate:  date("2017-05-21"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Customer Service",
						AirDate:  date("2017-05-28"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Patent Troll",
						AirDate:  date("2017-06-04"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "The Keenan Vortex",
						AirDate:  date("2017-06-11"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Hooli-Con",
						AirDate:  date("2017-06-18"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Server Error",
						AirDate:  date("2017-06-25"),
					},
				},
			},
			{
				ID:         "",
				SeriesID:   "",
				Title:      "Season 5",
				FirstAired: date("2018-03-25"),
				LastAired:  date("2018-05-13"),
				Episodes: []Episode{
					{
						ID:       "",
						SeasonID: "",
						Title:    "Grow Fast or Die Slow",
						AirDate:  date("2018-03-25"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Reorientation",
						AirDate:  date("2018-04-01"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Chief Operating Officer",
						AirDate:  date("2018-04-08"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Tech Evangelist",
						AirDate:  date("2018-04-15"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Facial Recognition",
						AirDate:  date("2018-04-22"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Artificial Emotional Intelligence",
						AirDate:  date("2018-04-29"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Initial Coin Offering",
						AirDate:  date("2018-05-06"),
					},
					{
						ID:       "",
						SeasonID: "",
						Title:    "Fifty-One Percent",
						AirDate:  date("2018-05-13"),
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
