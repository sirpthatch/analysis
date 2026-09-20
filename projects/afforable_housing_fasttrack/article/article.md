# **Front-Running the Fast Track**

Twelve neighborhoods are about to loose their City Council oversight of their own land use. On October 1st, 2026 the NYC Department of City Planning is due to release its list of the twelve community districts that produce the lowest rate of affordable housing, thanks to a [ballot measure](https://ballotpedia.org/New_York,_New_York,_Question_2,_Expedited_Public_Process_for_Affordable_Housing_Charter_Amendment_(November_2025)) that we voted in back in November, 2025. Some neighborhoods (ahem.. Bay Ridge) are already getting ahead of this by introducing their own rezoning plans ahead of the release. Since the methodology and data are mostly publicly accessible, let's see if we can front-run their list!

By combing through [HPD](https://data.cityofnewyork.us/resource/hg8x-zxpr.json) and [DCP](https://data.cityofnewyork.us/resource/dbdt-5s7j.json) data, I came up with the following communities making the cut in this cycle:

![fast_track_map](/Users/thatcher/dev/analysis/projects/afforable_housing_fasttrack/article/lib/fast_track_map.png)

## What does being on the "Affordable Housing Fast Track" list mean?

The designation is an attempt to streamline the approval process for new affordable housing development in areas that need the biggest push. Developments in these areas will still be subjected to reviews from the community board, borough president, and city planning commission, but will not require approval from the city council. This should shorten the land use permit process from 7 months to about 3.

The list is generated every 5 years and this is the first cycle. The list released on October 1st, 2026 will go into effect on Jan 1, 2027. The process is aimed at reducing friction, and time will tell if it is effective at increasing the stock of affordable housing.

## Rate Versus Count

The Upper West Side and Upper East Side are both on the projected list, despite the fact they have built a large quantity of affordable housing units. The Upper East Side produced 150 units this cycle, more than any other community on this list (though not the highest overall). The Upper West and East sides are the two largest communities by housing stock in NYC though, and because we are using a rate (net new affordable production / all housing), having a lot of "all housing" and not building much more means the rate will likely always be low.

This is a little counter-intuitive - even though they produce a lot of affordable housing they will be tagged as low production and be part of the fast track process. The use of rate versus count was almost certainly intentional, as it was included in our ballot measure.

Note, the Upper West Side came down to the line - a project that would add 84 affordable housing units and lift it out of the set of the 12 lowest producing neighborhoods got its DOB permits just 15 days after the Q2 2026 deadline. It will be interesting to see if the Department of City Planning gives "credit" for that one.

## Existing Stock of Affordable Housing

The methodology ignores existing affordable housing entirely - it only considers the net new production of affordable homes. So neighborhoods that already provide many options for affordable housing are treated the same as ones that have none.

If we factor in the existing stock to the rate, most neighborhoods in Manhattan would fall off and more neighborhoods in the outer boroughs would be part of the list:

![stock_comparison_map](/Users/thatcher/dev/analysis/projects/afforable_housing_fasttrack/article/lib/stock_comparison_map.png)



## A Thought on "Affordable"

In NYC a housing unit occupied by a family earning $250,000 is treated the same as a unit for a family earning $46,000. In fact, "affordable" in the eyes of this policy includes units designated for anyone earning up to **165% of the median income** (and median income actually includes incomes from some wealthy suburbs of NYC). A big reason for this is to make it attractive for private real estate developers to build units (since they can capture a high margin on more expensive units). And structuring it this way does help city professionals earning mid-range salaries land rent-stabilized units.

Unsurprisingly, 39% of the affordable housing produced is for people making 121-165% of the median income, the largest band. And in some communities that is all that is being built - in Ridgewood and Canarsie 100% of the affordable units are for people making 121-165% of the median income. In other communities the opposite is true - the Upper West Side's production is 47% for families at or below 80% of the median income, and the Upper East Side is at 37% for the same.

It is hard not to look at that though and feel like people at the lower salary tiers, people truly below the median income, got short-changed here. When people voted for affordable housing in 2025, they might have had these people in mind, but this policy is not focused on them. We should do more.  

## Cavets

Some necessary caveats:

* There are many records where the address is redacted (likely for privacy reasons), and once those are included it will affect the rates of affordable housing developed in these communities (particularly the ones with 0% currently). I do not expect it will affect the set of 12 though, rather just move them around in their rankings. When I ran some extreme inclusion/exclusion scenarios the list of 12 was the same, just in a different order.
* We are missing one quarter of development data, which could also affect the number of affordable houses developed. Again, I do not expect this to change the set - the only possible candidate would be for Bayside moving out of the set, and Bensonhurst (BK-11) moving in, based on crediting Bayside with its "best" construction quarter in place of the one we are missing.
