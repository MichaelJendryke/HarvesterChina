using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NetDimension.Weibo;
//using System.Web.Script.Serialization;//deserialize JSON
using System.Runtime.Serialization;
//using System.Runtime.Serialization.Json;
using System.IO;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;
using MySql.Data.Types;
using MySql.Data;
using Newtonsoft.Json;
using Npgsql;
using System.Net;
using System.Linq;
using System.Transactions;
using System.Dynamic;


// Local Branch Michael Jendryke 2015-03-02 7:17

namespace HarvesterChina
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine("Welcome");

            string logbook = "<--- Time the program starts";
            toLOG(logbook);
            do
            {
                //VARIABLES
                int h = 2477; //number of hexagons for this harvester
                int updateroundstoMSSQL = 50;   //number of rounds until update to MSSQL
                int past = 4;    //number of full month to go back in time (start)


                //Get hexagons
                lists.Chexagons<int, int, int, int, double, double, double> Chexagons = SQLServer.GetHexagonsFromSQL(h);
                int CountHexagons = Chexagons.Count();

                ////Output on console
                //for (int i = 0; i < CountHexagons; i++)
                //{

                //    Console.WriteLine(Chexagons[i].Item1 + "\t" +
                //                      Chexagons[i].Item2 + "\t" +
                //                      Chexagons[i].Item3 + "\t" +
                //                      Chexagons[i].Item4 + "\t" +
                //                      Chexagons[i].Item5 + "\t" +
                //                      Chexagons[i].Item6 + "\t" +
                //                      Chexagons[i].Item7
                //                      );
                //}
                Console.WriteLine(CountHexagons.ToString() + " \tHexagons");

                //mark hexagons as taken
                SQLServer.SetHexagonsAsTakenSQL(Chexagons);

                //Make probability array
                List<int> probability = new List<int>();
                int total = 0;
                for (int i = 0; i < CountHexagons; i++)
                {
                    int c = Convert.ToInt16(Math.Truncate(Chexagons[i].Item7 * 100));
                    for (int j = 0; j < c; j++)
                    {
                        probability.Add((int)(Chexagons[i].Item1));
                        total = total + 1;

                    }

                }
                Console.Write(probability.Count().ToString() + " \t'fields' in probability array \n");



                //Get Harvester AppKeys
                lists.AppKeysAndSecrets<int, string, string, string, string> AppKeysAndSecretsList = SQLServer.GetAppKeysFromSQL();

                //Number of Harvesters is the number of AppKeys
                int numHarvesters = AppKeysAndSecretsList.Count();
                Console.Write(numHarvesters.ToString() + " \tAppKeys \n");
                SQLServer.SetAppKeysAsTakenSQL(AppKeysAndSecretsList);
                //Set Harvesters as active
                for (int i = 0; i < numHarvesters; i++)
                {

                    Console.WriteLine(AppKeysAndSecretsList[i].Item1 + "\t" +
                                      AppKeysAndSecretsList[i].Item2 + "\t" +
                                      AppKeysAndSecretsList[i].Item3 + "\t" +
                                      AppKeysAndSecretsList[i].Item4 + "\t" +
                                      AppKeysAndSecretsList[i].Item5
                                      );
                }




                int rounds = 0;
                int updaterounds = 0;
                int res = 0;
                int totalcollected = 1;
                int totalinserted = 0;


                Stopwatch runTime = new Stopwatch();
                runTime.Start();
                Stopwatch UpdateTime = new Stopwatch();
                UpdateTime.Start();

                while (1 > 0)
                {

                    //Console.WriteLine("LET's roll!");
                    Stopwatch stopWatch = new Stopwatch();
                    stopWatch.Start();


                    rounds = rounds + 1;
                    double percent = (double)(100 / totalcollected) * totalinserted;
                    double resultsperrequest = (double)totalinserted / (rounds * 20);
                    int rr = rounds * numHarvesters; //requests
                    double hh = runTime.Elapsed.TotalSeconds / 3600; // total hours
                    double effective = (double)rounds / hh;

                    Console.Title = ("Round " + rounds.ToString() + "/" + updaterounds.ToString()
                        + " Results in last round: " + res.ToString()
                        + " collected: " + totalcollected.ToString()
                        + " inserted: " + totalinserted.ToString()
                        + " Records inserted per request: " + resultsperrequest)
                        + " Runtime: " + runTime.Elapsed.ToString("dd' day(s) 'hh':'mm':'ss")
                        + " Requests per hour " + effective.ToString();

                    res = 0;

                    //Update probabilities every 15 minutes
                    //TimeSpan tsut = UpdateTime.Elapsed;
                    //int ut = Convert.ToInt32(tsut.TotalSeconds);
                    //if (ut > 900) //15 minutes = 900 seconds
                    //{
                    //Console.WriteLine("\n15 minutes over: recalculate probabilities");
                    //UpdateTime.Restart();
                    //Console.Write("\nFields: " + Chexagons.Count().ToString());
                    //Console.Write("old probabilities: " + probability.Count().ToString());
                    //Make probability array
                    probability.RemoveRange(0, probability.Count());
                    total = 0;
                    for (int i = 0; i < CountHexagons; i++)
                    {
                        int c = Convert.ToInt16(Math.Truncate(Chexagons[i].Item7 * 100));
                        for (int j = 0; j < c; j++)
                        {
                            probability.Add((int)(Chexagons[i].Item1));
                            total = total + 1;
                        }
                    }
                    //Console.Write("new probabilities: " + probability.Count().ToString()+"\n");
                    //}
                    //Update field table to MSSQL table [weibo].[dbo].[HEXAGONPOINTS] 

                    updaterounds = updaterounds + 1;
                    if (updaterounds > updateroundstoMSSQL)
                    {
                        //WeiboMySQL.UpdateHexagonsFromMSSQL(Chexagons);
                        updaterounds = 0;
                    }

                    //Start and end time
                    DateTime start = new DateTime(2013, 12, 30, 0, 0, 0);
                    //DateTime end = new DateTime(2015, 1, 1, 23, 0, 0);
                    DateTime end = DateTime.Now;
                    start = end.AddMonths(-past);
                    start = start.AddDays(-end.Day);
                    start = start.AddHours(-end.Hour);
                    start = start.AddMinutes(-end.Minute);
                    start = start.AddSeconds(-end.Second);

                    //Console.WriteLine(start);
                    //Console.WriteLine(end);
                    //Console.WriteLine((int)DateTimeToUnixTimestamp(start));
                    //Console.WriteLine((int)DateTimeToUnixTimestamp(end));







                    // LOOP THROUGH all Harvesters
                    for (int currentHarvester = 0; currentHarvester < numHarvesters; currentHarvester++)
                    {


                        //Console.WriteLine(start);
                        //Console beginning of request
                        //Console.Write("AppKey: " + String.Format("{0,-10}", AppKeysAndSecretsList[currentHarvester].Item2));

                        //Pick random field
                        Random rnd = new Random();
                        int pidx = rnd.Next(0, probability.Count()); //pidx is the index of the large array
                        int Oidx = probability[pidx];                //Oidx is the Number that is stored at the pidx Index
                        int Chidx = 0;                               //Chidx is the INDEX of the ChexagonList that has the Oidx
                        for (int i = 0; i < Chexagons.Count; i++)
                        {
                            if (Chexagons[i].Item1 == Oidx)
                            {
                                Chidx = i;
                            }
                        }


                        //Make random time
                        Random gen = new Random();
                        //DateTime t = start.AddMinutes(gen.Next(range));



                        int eT = rnd.Next((int)DateTimeToUnixTimestamp(start), (int)DateTimeToUnixTimestamp(end));
                        Console.Write("\n" + (currentHarvester + 1) + "\t" + UNIXtimeTOreadable(eT).ToString("yyyy/MM/dd HH:mm:ss") +
                            " AppKEY: " + String.Format("{0,12:###}", AppKeysAndSecretsList[currentHarvester].Item2));

                        //Get Authorization

                        OAuth oauth = null;
                        string accessToken = Properties.Settings.Default.AcessToken;
                        try
                        {
                            oauth = Authorize(AppKeysAndSecretsList[currentHarvester].Item2,
                                              AppKeysAndSecretsList[currentHarvester].Item3,
                                              AppKeysAndSecretsList[currentHarvester].Item4,
                                              AppKeysAndSecretsList[currentHarvester].Item5);
                        }
                        catch (Exception ex)
                        {
                            //Console.Write(ex.ToString());
                            //Console.WriteLine(ex.Message);

                            continue; // is much better than break
                        }

                        if (!string.IsNullOrEmpty(oauth.AccessToken))
                        {
                            Console.Write(" Access!");
                        }
                        else
                        {
                            Console.Write(" DAMN no Access!");

                        }

                        //Sina is your friend!
                        Client Sina = new Client(oauth);

                        //.Net4.0
                        try
                        {

                            //这里注意，.Net4.0的SDK和其他版本的SDK调用API的时候稍有不同
                            //.Net4.0
#if NET40
                            // NTL = Nearby TimeLine - contains the resulting JSON object
                            dynamic NTL = new System.Dynamic.ExpandoObject();

                            int rng = (int)11132; //range around Lat/Long
                            int cnt = (int)50; //Count
                            double LAT = Chexagons[Chidx].Item5;
                            double LON = Chexagons[Chidx].Item6;
                            Random randomoffset = new Random();
                            double offsetA = (randomoffset.Next(-2500, 2500)) * 0.000001;
                            double offsetO = (randomoffset.Next(-2500, 2500)) * 0.000001;

                            LAT = LAT + offsetA;
                            LON = LON + offsetO;

                            Console.Write(" LAT: " + String.Format("{0,7:###.000}", LAT) + " and LON: " + String.Format("{0,7:###.000}", LON) + " F:" + String.Format("{0,6:###}", Chexagons[Chidx].Item1));

                            //Request to SINA
                            try
                            {
                                Console.Write(" get");
                                NTL = Sina.GetCommand("https://api.weibo.com/2/place/nearby_timeline.json",
                                       new WeiboParameter("lat", LAT),
                                       new WeiboParameter("long", LON),
                                       new WeiboParameter("range", rng),// maximum radius is 11132m
                                    // new WeiboParameter("starttime", sT),
                                       new WeiboParameter("endtime", eT),
                                    //new WeiboParameter("sort", false),
                                       new WeiboParameter("count", cnt)//50 is the limit
                                    //new WeiboParameter("page", 1),
                                    //new WeiboParameter("base_app", false),
                                    //new WeiboParameter("offset", false)
                                       );
                                Console.Write("NBT");


                            }
                            catch (Exception ex)
                            {
                                Console.Write(ex.ToString());
                                Console.WriteLine(ex.Message);

                                continue; // is much better than break
                            }

                            //Console.Write( Sina.API.Dynamic.Account.RateLimitStatus());
                            string AccessToken = Sina.OAuth.AccessToken.ToString();

                            dynamic dNTL = new ExpandoObject();
                            try
                            {
                                //Deserialize the JSON with https://json.codeplex.com/releases/view/121470
                                
                                dNTL = JsonConvert.DeserializeObject(NTL);
                                Console.Write(" dS");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }


                            

                            //Parameteriszed MySQL Insert Ignore Season ID for all of China is 8 and fieldgroup ID is also 8
                            //var good = NearPubTimelineToSQL(dNTL,
                            //                      "8",
                            //                      Chexagons[Chidx].Item1.ToString(),
                            //                      8);
                            int collected = 0;
                            int inserted = 0;
                            int statusescount = 0;
                            try
                            {
                                statusescount = dNTL["statuses"].Count;
                                //Console.Write("Count is:" + statusescount);
                            }
                            catch (Exception ex)
                            {
                                //Console.WriteLine(ex);
                                statusescount = 0;
                                //Console.Write("Count is:" + statusescount);
                            }


                            if (statusescount > 0)
                            {
                                var r = SQLServer.NearbyTimelineToSQL(dNTL,
                                                      "8",
                                                      Chexagons[Chidx].Item1.ToString(),
                                                      8);
                                collected = r.Item1;
                                inserted = r.Item2;

                                Console.Write(" Result = [" + String.Format("{0,2:##0}", collected) + "/" + String.Format("{0,2:##0}", inserted) + "] ");
                                totalcollected = totalcollected + collected;
                                totalinserted = totalinserted + inserted;
                            }
                            else {
                                Console.Write(" Result = [     ] ");
                            }


                            //UPDATE Chexagons

                            //TimesHarvested  TotalInserted
                            int RandomID = Chexagons[Chidx].Item1;
                            int TimesHarvested = Chexagons[Chidx].Item2 + 1;
                            int TotalCollected = Chexagons[Chidx].Item3 + collected;
                            int TotalInserted = Chexagons[Chidx].Item4 + inserted;
                            double LA = Chexagons[Chidx].Item5;
                            double LO = Chexagons[Chidx].Item6;
                            double ratio = 0.01;
                            if (TotalInserted > 0)
                            {
                                ratio = (double)TotalInserted / (double)TotalCollected;
                                //Console.WriteLine("PROBLEM" + String.Format("{0,5:#0.00}", ratio));
                                if (ratio < 0.01)
                                {
                                    ratio = 0.01;
                                }
                                if (ratio > 0.99)
                                {
                                    ratio = 0.99;
                                }

                            }
                            else
                            {

                            }

                            //Console.WriteLine("\n" + Chexagons[Chidx].Item1.ToString() + " " + Chexagons[Chidx].Item2.ToString() + " " + Chexagons[Chidx].Item3.ToString() + " " + Chexagons[Chidx].Item4.ToString() + " " + Chexagons[Chidx].Item5.ToString() + " " + Chexagons[Chidx].Item6.ToString() + " " + Chexagons[Chidx].Item7.ToString());
                            Chexagons[Chidx] = Tuple.Create(RandomID, TimesHarvested, TotalCollected, TotalInserted, LA, LO, ratio);
                            WeiboMySQL.UpdateSingleHexagonFromMSSQL(RandomID, TimesHarvested, TotalCollected, TotalInserted, ratio);
                            //Console.WriteLine(Chexagons[Chidx].Item1.ToString() + " " + Chexagons[Chidx].Item2.ToString() + " " + Chexagons[Chidx].Item3.ToString() + " " + Chexagons[Chidx].Item4.ToString() + " " + Chexagons[Chidx].Item5.ToString() + " " + Chexagons[Chidx].Item6.ToString() + " " + Chexagons[Chidx].Item7.ToString());



                            //.Net4.0
#else
                                    //.Net其他版本
                                    string uid = Sina.API.Account.GetUID();	//获取UID
                                    //这里用VS2010的var关键字和可选参数最惬意了。
                                    //如果用VS2005什么的你得这样写：
                                    //NetDimension.Weibo.Entities.user.Entity userInfo = Sina.API.Users.Show(uid,null);
                                    //如果用VS2008什么的也不方便，你得把参数写全：
                                    //var userInfo = Sina.API.Users.Show(uid,null);
                                    var userInfo = Sina.API.Users.Show(uid);
                                    Console.WriteLine("昵称：{0}", userInfo.ScreenName);
                                    Console.WriteLine("来自：{0}", userInfo.Location);

                                    //发条微博啥的

                                    var statusInfo = Sina.API.Statuses.Update(string.Format("我是{0}，我来自{1}，我在{2}用《新浪微博开放平台API for .Net SDK》开发了一个小程序并发了这条微博，欢迎关注http://weibosdk.codeplex.com/", userInfo.ScreenName, userInfo.Location, DateTime.Now.ToShortTimeString()));
                                    //加个当前时间防止重复提交报错。

                                    Console.WriteLine("微博已发送，发送时间：{0}", statusInfo.CreatedAt);

#endif
                        }
                        catch (Exception ex)
                        {
                        }




                    }//END OF FOR LOOP AppKeys
                    // Get the elapsed Stopwatch time as a TimeSpan value.
                    stopWatch.Stop();
                    TimeSpan ts = stopWatch.Elapsed;
                    int maxNumOfRequests = 150;
                    int oneHourInMilliseconds = 3600000;
                    int FixRoundSeconds = (oneHourInMilliseconds / maxNumOfRequests) / 1000;
                    int roundTime = Convert.ToInt32(ts.TotalSeconds);
                    int wait = FixRoundSeconds - roundTime;
                    if (wait < 1)
                    {
                        wait = 1;
                    }


                    if (effective > maxNumOfRequests)
                    {
                        //Write Log
                        Console.WriteLine("\n LOG " +
                                           DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") +
                                           " last round " + roundTime + "s" +
                                           " please wait " + wait + "s for next one..." +
                                                                                  "");

                        //Wait 
                        System.Threading.Thread.Sleep(wait * 1000); //Milliseconds
                    }

                }

            } while (10 > 1);

        } // END OF MAIN



        //Other functions
        static OAuth Authorize(string Key, string Secret, string pp, string pw)
        {
            //OAuth o = new OAuth(Properties.Settings.Default.AppKey, Properties.Settings.Default.AppSecret, Properties.Settings.Default.CallbackUrl);
            OAuth o = new OAuth(Key, Secret, Properties.Settings.Default.CallbackUrl);
            //string passport = "michael@jendryke.de";
            //string password = "12345678";

            while (!ClientLogin(o, pp, pw))	//使用模拟方法//调用ClientLogin函数
            {
                Console.WriteLine("No Access :-( ");
                break;
            }


            return o;
        }

        private static bool ClientLogin(OAuth o, string passport, string password)
        {


            bool works = false;
            try
            {
                works = o.ClientLogin(passport, password);
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);

            }
            return works;
        }

        public static string NearPubTimelineToSQLOLD(dynamic S, string SID, string FID)
        {
            //var bounds = (string)S["bounds"];
            string values = "";
            //Create parameterized INSERT IGNORE


            foreach (var result in S["statuses"])
            {

                //Console.WriteLine(result["geo"]["coordinates"][0]);
                string createdAT = (string)result["created_at"];
                DateTime myDate = DateTime.ParseExact(createdAT.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy", System.Globalization.CultureInfo.InvariantCulture);
                //Console.WriteLine(myDate.ToString("yyyy-MM-dd HH:mm:ss zzzzz"));
                createdAT = myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                string id = (string)result["id"];
                string mid = (string)result["mid"];
                //Test if Geo_Enabled is true
                string geoEnabled = (string)result["user"]["geo_enabled"];

                string geoTYPE = (string)"";
                string geoLAT = (string)"";
                string geoLOG = (string)"";

                if (String.Equals(geoEnabled, "true", StringComparison.InvariantCultureIgnoreCase))
                {
                    geoTYPE = (string)result["geo"]["type"];
                    geoLAT = ((float)result["geo"]["coordinates"][0]).ToString("000.000000");
                    geoLOG = ((float)result["geo"]["coordinates"][1]).ToString("000.000000");
                    geoEnabled = "1";
                }
                else if (String.Equals(geoEnabled, "false", StringComparison.InvariantCultureIgnoreCase))
                {
                    geoTYPE = "N/A";
                    geoLAT = "0.0";
                    geoLOG = "0.0";
                    geoEnabled = "0";
                }
                string userID = (string)result["user"]["id"];
                string distance = (string)result["distance"];

                string msgidstr = (string)result["idstr"];
                string msgtext = (string)result["text"];
                msgtext = msgtext.Replace("'", @"\'");
                string msgin_reply_to_status_id = (string)result["in_reply_to_status_id"];
                if (msgin_reply_to_status_id == "")
                {
                    msgin_reply_to_status_id = "0";
                }
                string msgin_reply_to_user_id = (string)result["in_reply_to_user_id"];
                if (msgin_reply_to_user_id == "")
                {
                    msgin_reply_to_user_id = "0";
                }
                string msgin_reply_to_screen_name = (string)result["in_reply_to_screen_name"];
                string msgfavorited = (string)result["favorited"];
                if (msgfavorited == "false")
                {
                    msgfavorited = "0";
                }
                else
                {
                    msgfavorited = "1";
                }

                string userscreen_name = (string)result["user"]["screen_name"];
                userscreen_name = userscreen_name.Replace("'", @"\'");
                string userprovince = (string)result["user"]["province"];
                string usercity = (string)result["user"]["city"];
                string userlocation = (string)result["user"]["location"];
                string userdescription = (string)result["user"]["description"];
                string userfollowers_count = (string)result["user"]["followers_count"];
                string userfriends_count = (string)result["user"]["friends_count"];
                string userstatuses_count = (string)result["user"]["statuses_count"];
                string userfavourites_count = (string)result["user"]["favourites_count"];
                string usercreated_at = (string)result["user"]["created_at"];
                myDate = DateTime.ParseExact(usercreated_at.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy", System.Globalization.CultureInfo.InvariantCulture);
                usercreated_at = myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                string usergeo_enabled = geoEnabled;
                string userverified = (string)result["user"]["verified"];
                if (userverified == "false")
                {
                    userverified = "0";
                }
                else
                {
                    userverified = "1";
                }
                string userbi_followers_count = (string)result["user"]["bi_followers_count"];
                string userlang = (string)result["user"]["lang"];
                string userclient_mblogid = (string)result["user"]["client_mblogid"];

                values = values + "(" +
                    "'" + SID + "'," +
                    "'" + FID + "'," +
                    "'" + createdAT + "'," +
                    "'" + id + "'," +
                    "'" + mid + "'," +
                    "'" + geoTYPE + "'," +
                    "'" + geoLAT + "'," +
                    "'" + geoLOG + "'," +
                    "'" + userID + "'," +
                    "'" + distance + "'," +
                    "GeomFromText('POINT(" + geoLOG + " " + geoLAT + ")')," + //this is the actual POINT
                    "'" + msgidstr + "'," +
                    "'" + msgtext + "'," +
                    "'" + msgin_reply_to_status_id + "'," +
                    "'" + msgin_reply_to_user_id + "'," +
                    "'" + msgin_reply_to_screen_name + "'," +
                    "'" + msgfavorited + "'," +
                    "'" + userscreen_name + "'," +
                    "'" + userprovince + "'," +
                    "'" + usercity + "'," +
                    "'" + userlocation + "'," +
                    //"'" + userdescription + "'," +
                    "'" + userfollowers_count + "'," +
                    "'" + userfriends_count + "'," +
                    "'" + userstatuses_count + "'," +
                    "'" + userfavourites_count + "'," +
                    "'" + usercreated_at + "'," +
                    "'" + usergeo_enabled + "'," +
                    "'" + userverified + "'," +
                    "'" + userbi_followers_count + "'," +
                    "'" + userlang + "'," +
                    "'" + userclient_mblogid + "'" +
                    "),";



            }
            string SQLstring = "INSERT IGNORE into nearbytimeline (" +
                "SeasonID," +
                "FieldID," +
                "createdAT," +
                "msgID," +
                "msgmid," +
                "geoTYPE," +
                "geoLAT," +
                "geoLOG," +
                "userID," +
                "distance," +
                "Point," +
                "msgidstr," +
                "msgtext," +
                "msgin_reply_to_status_id," +
                "msgin_reply_to_user_id," +
                "msgin_reply_to_screen_name," +
                "msgfavorited," +
                "userscreen_name," +
                "userprovince," +
                "usercity," +
                "userlocation," +
                //"userdescription," +
                "userfollowers_count," +
                "userfriends_count," +
                "userstatuses_count," +
                "userfavourites_count," +
                "usercreated_at," +
                "usergeo_enabled," +
                "userverified," +
                "userbi_followers_count," +
                "userlang," +
                "userclient_mblogid" +
                ") values ";
            values = values.TrimEnd(',');
            SQLstring = SQLstring + values;


            return SQLstring;


        }

        public static Tuple<int, int, int> NearPubTimelineToSQL(dynamic S, string SID, string FID, int FGID)
        {

            int answer = 0;
            int wentin = 0;
            int c = 0;
            //Create parameterized INSERT IGNORE
            //MySQL connection
            MySqlConnection conn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand cmd = conn.CreateCommand();

            cmd.CommandText = "INSERT IGNORE into nearbytimeline(" +
                                                                 "SeasonID," +
                                                                 "FieldID," +
                                                                 "FieldGroupID," +
                                                                 "createdAT," +
                                                                 "msgID," +
                                                                 "msgmid," +
                                                                 "geoTYPE," +
                                                                 "geoLAT," +
                                                                 "geoLOG," +
                                                                 "userID," +
                                                                 "distance," +
                //InnoDB// "Point," +
                                                                 "msgidstr," +
                                                                 "msgtext," +
                                                                 "msgin_reply_to_status_id," +
                                                                 "msgin_reply_to_user_id," +
                                                                 "msgin_reply_to_screen_name," +
                                                                 "msgfavorited," +
                                                                 "userscreen_name," +
                                                                 "userprovince," +
                                                                 "usercity," +
                                                                 "userlocation," +
                                                                 "userfollowers_count," +
                                                                 "userfriends_count," +
                                                                 "userstatuses_count," +
                                                                 "userfavourites_count," +
                                                                 "usercreated_at," +
                                                                 "usergeo_enabled," +
                                                                 "userverified," +
                                                                 "userbi_followers_count," +
                                                                 "userlang," +
                                                                 "userclient_mblogid" +
                //"userdescription" +
                                                                 ")" +

                                                          "VALUES(" +
                                                                  "@SID," +
                                                                  "@FID," +
                                                                  "@FGID," +
                                                                  "@createdAT," +
                                                                  "@msgid," +
                                                                  "@msgmid," +
                                                                  "@geoTYPE," +
                                                                  "@geoLAT," +
                                                                  "@geoLOG," +
                                                                  "@userID," +
                                                                  "@distance," +
                //InnoDB// "GeomFromText(@Point)," +
                                                                  "@msgidstr," +
                                                                  "@msgtext," +
                                                                  "@msgin_reply_to_status_id," +
                                                                  "@msgin_reply_to_user_id," +
                                                                  "@msgin_reply_to_screen_name," +
                                                                  "@msgfavorited," +
                                                                  "@userscreen_name," +
                                                                  "@userprovince," +
                                                                  "@usercity," +
                                                                  "@userlocation," +
                                                                  "@userfollowers_count," +
                                                                  "@userfriends_count," +
                                                                  "@userstatuses_count," +
                                                                  "@userfavourites_count," +
                                                                  "@usercreated_at," +
                                                                  "@usergeo_enabled," +
                                                                  "@userverified," +
                                                                  "@userbi_followers_count," +
                                                                  "@userlang," +
                                                                  "@userclient_mblogid" +
                //"@userdescription" +
            ")";

            try
            {


                conn.Open();
                foreach (var result in S["statuses"])
                {
                    c = c + 1;
                    try
                    {
                        cmd.Parameters.AddWithValue("@SID", Convert.ToInt32(SID));
                        cmd.Parameters.AddWithValue("@FID", Convert.ToInt32(FID));
                        cmd.Parameters.AddWithValue("@FGID", Convert.ToInt32(FGID));

                        string createdAT = (string)result["created_at"];
                        DateTime myDate = DateTime.ParseExact(createdAT.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy", System.Globalization.CultureInfo.InvariantCulture);
                        createdAT = myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                        cmd.Parameters.AddWithValue("@createdAT", createdAT);


                        cmd.Parameters.AddWithValue("@msgid", result["id"]);
                        //Console.WriteLine(result["id"]);
                        cmd.Parameters.AddWithValue("@msgmid", result["mid"]);

                        //Test if Geo_Enabled is true
                        string geoEnabled = (string)result["user"]["geo_enabled"];
                        if (String.Equals(geoEnabled, "true", StringComparison.InvariantCultureIgnoreCase))
                        {
                            cmd.Parameters.AddWithValue("@geoTYPE", (string)result["geo"]["type"]);
                            cmd.Parameters.AddWithValue("@geoLAT", result["geo"]["coordinates"][0]);
                            cmd.Parameters.AddWithValue("@geoLOG", result["geo"]["coordinates"][1]);
                            cmd.Parameters.AddWithValue("@usergeo_enabled", 1);
                            //cmd.Parameters.AddWithValue("@Point", "POINT(121.310383 31.012271)");
                            //InnoDB//cmd.Parameters.AddWithValue("@Point", "POINT(" + result["geo"]["coordinates"][1] + " " + result["geo"]["coordinates"][0] + ")");
                        }
                        else if (String.Equals(geoEnabled, "false", StringComparison.InvariantCultureIgnoreCase))
                        {
                            cmd.Parameters.AddWithValue("@geoTYPE", "N/A");
                            cmd.Parameters.AddWithValue("@geoLAT", "0.0");
                            cmd.Parameters.AddWithValue("@geoLOG", "0.0");
                            cmd.Parameters.AddWithValue("@usergeo_enabled", 0);
                            //InnoDB//cmd.Parameters.AddWithValue("@Point", "POINT(0.0 0.0)");
                        }


                        cmd.Parameters.AddWithValue("@userID", result["user"]["id"]);
                        cmd.Parameters.AddWithValue("@distance", result["distance"]);
                        cmd.Parameters.AddWithValue("@msgidstr", result["idstr"]);
                        cmd.Parameters.AddWithValue("@msgtext", (string)result["text"]);

                        cmd.Parameters.AddWithValue("@msgin_reply_to_status_id", result["in_reply_to_status_id"]);
                        cmd.Parameters.AddWithValue("@msgin_reply_to_user_id", result["in_reply_to_user_id"]);
                        cmd.Parameters.AddWithValue("@msgin_reply_to_screen_name", result["in_reply_to_screen_name"]);
                        cmd.Parameters.AddWithValue("@msgfavorited", result["favorited"]);
                        cmd.Parameters.AddWithValue("@userscreen_name", (string)result["user"]["screen_name"]);

                        cmd.Parameters.AddWithValue("@userprovince", result["user"]["province"]);

                        cmd.Parameters.AddWithValue("@usercity", result["user"]["city"]);
                        cmd.Parameters.AddWithValue("@userlocation", result["user"]["location"]);

                        cmd.Parameters.AddWithValue("@userfollowers_count", result["user"]["followers_count"]);
                        cmd.Parameters.AddWithValue("@userfriends_count", result["user"]["friends_count"]);
                        cmd.Parameters.AddWithValue("@userstatuses_count", result["user"]["statuses_count"]);
                        cmd.Parameters.AddWithValue("@userfavourites_count", result["user"]["favourites_count"]);
                        string usercreated_at = result["user"]["created_at"];
                        myDate = DateTime.ParseExact(usercreated_at.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy", System.Globalization.CultureInfo.InvariantCulture);
                        cmd.Parameters.AddWithValue("@usercreated_at", myDate.ToUniversalTime());

                        cmd.Parameters.AddWithValue("@userverified", result["user"]["verified"]);

                        cmd.Parameters.AddWithValue("@userbi_followers_count", result["user"]["bi_followers_count"]);
                        cmd.Parameters.AddWithValue("@userlang", result["user"]["lang"]);
                        cmd.Parameters.AddWithValue("@userclient_mblogid", result["user"]["client_mblogid"]);
                        //cmd.Parameters.AddWithValue("@userdescription", result["user"]["description"]);

                        int a = cmd.ExecuteNonQuery();
                        cmd.Parameters.Clear();
                        if (a == 1)
                        {
                            wentin = wentin + 1;
                        }


                    }
                    catch (Exception ex)
                    {
                        cmd.Parameters.Clear();
                        //Console.Write(" :-( ");
                        c = c - 1;
                        //Console.Write(ex.ToString());
                        //Console.WriteLine(ex.Message);
                        answer = 1;
                        continue;
                    }
                }//END FOR EACH
                conn.Close();
                //Console.Write(" #" + wentin + " ");
                //Console.Write(" Inserted " + String.Format("{0:00}", wentin));
                //return wentin;

            }
            catch (Exception ex)
            {
                //Console.Write(ex.ToString());
                //Console.WriteLine(ex.Message);
                answer = 1;
            }
            var aaaa = new Tuple<int, int, int>(answer, c, wentin);

            return aaaa;
        }

        public static int NearPubTimelineToTimoServer(dynamic s, int sid, int fid, int fgid)
        {
            const string server = "timo-server";
            const string database = "weibo_raw";

            const string sqlUser = "michael";
            const string sqlPwd = "weibo1234";

            const string connectionString = "Server=" + server + ";Database=" + database + ";User ID=" + sqlUser +
                                            ";Password=" + sqlPwd + ";Trusted_Connection=False";



            int written = 0;
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();


                const string insertCommand = "INSERT INTO dbo.Harvest_Backup(" +
                                             "SeasonID," +
                                             "FieldID," +
                                             "FieldGroupID," +
                                             "createdAT," +
                                             "msgID," +
                                             "msgmid," +
                                             "geoTYPE," +
                                             "geoLAT," +
                                             "geoLOG," +
                                             "userID," +
                                             "distance," +
                    //InnoDB// "Point," +
                                             "msgidstr," +
                                             "msgtext," +
                                             "msgin_reply_to_status_id," +
                                             "msgin_reply_to_user_id," +
                                             "msgin_reply_to_screen_name," +
                                             "msgfavorited," +
                                             "userscreen_name," +
                                             "userprovince," +
                                             "usercity," +
                                             "userlocation," +
                                             "userfollowers_count," +
                                             "userfriends_count," +
                                             "userstatuses_count," +
                                             "userfavourites_count," +
                                             "usercreated_at," +
                                             "usergeo_enabled," +
                                             "userverified," +
                                             "userbi_followers_count," +
                                             "userlang," +
                                             "userclient_mblogid" +
                    //"userdescription" +
                                             ")" +

                                             "SELECT " +
                                             "@SID," +
                                             "@FID," +
                                             "@FGID," +
                                             "@createdAT," +
                                             "@msgid," +
                                             "@msgmid," +
                                             "@geoTYPE," +
                                             "@geoLAT," +
                                             "@geoLOG," +
                                             "@userID," +
                                             "@distance," +
                    //InnoDB// "GeomFromText(@Point)," +
                                             "@msgidstr," +
                                             "@msgtext," +
                                             "@msgin_reply_to_status_id," +
                                             "@msgin_reply_to_user_id," +
                                             "@msgin_reply_to_screen_name," +
                                             "@msgfavorited," +
                                             "@userscreen_name," +
                                             "@userprovince," +
                                             "@usercity," +
                                             "@userlocation," +
                                             "@userfollowers_count," +
                                             "@userfriends_count," +
                                             "@userstatuses_count," +
                                             "@userfavourites_count," +
                                             "@usercreated_at," +
                                             "1," +
                                             "@userverified," +
                                             "@userbi_followers_count," +
                                             "@userlang," +
                                             "@userclient_mblogid" +
                    //"@userdescription" +
                                             " WHERE NOT EXISTS ( SELECT msgId from dbo.Harvest_Backup WHERE msgId = @msgId)";

                ////                string insertCommand = "INSERT INTO dbo.ALL_Messages (idNearByTimeLine, msgId) SELECT @id, @msgId WHERE NOT EXISTS ( SELECT msgId from dbo.ALL_Messages WHERE msgId = @msgId)";
                //////                string insertCommand = "INSERT IGNORE INTO dbo.ALL_Messages (idNearByTimeLine) VALUES (@id)";

                ////                //string insertCommand = "INSERT INTO dbo.ALL_Messages (idNearByTimeLine, msgId) VALUES (@id, @msgId)";

                using (var cmd = new SqlCommand(insertCommand, conn))
                {
                    foreach (var result in s["statuses"])
                    {
                        var geoEnabled = (string)result["user"]["geo_enabled"];
                        if (String.Equals(geoEnabled, "true", StringComparison.InvariantCultureIgnoreCase))
                        {
                            //cmd.Parameters.Add("@id", System.Data.SqlDbType.Int).Value = 4;
                            cmd.Parameters.Add("@SID", System.Data.SqlDbType.Int).Value = sid;
                            cmd.Parameters.Add("@FID", System.Data.SqlDbType.Int).Value = fid;
                            cmd.Parameters.Add("@FGID", System.Data.SqlDbType.Int).Value = fgid;

                            var createdAt = (string)result["created_at"];
                            DateTime myDate = DateTime.ParseExact(createdAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                System.Globalization.CultureInfo.InvariantCulture);
                            //myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                            cmd.Parameters.Add("@createdAT", System.Data.SqlDbType.DateTime).Value =
                                myDate.ToUniversalTime();


                            cmd.Parameters.Add("@msgid", System.Data.SqlDbType.BigInt).Value =
                                long.Parse(Convert.ToString(result["id"]));
                            //Console.WriteLine(result["id"]);


                            cmd.Parameters.Add("@msgmid", System.Data.SqlDbType.BigInt).Value =
                                long.Parse(Convert.ToString(result["mid"]));


                            cmd.Parameters.Add("@geoTYPE", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["geo"]["type"]);
                            cmd.Parameters.Add("@geoLAT", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][0]));
                            cmd.Parameters.Add("@geoLOG", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][1]));

                            //cmd.Parameters.AddWithValue("@usergeo_enabled", 1);

                            cmd.Parameters.Add("@userID", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["user"]["id"]));
                            cmd.Parameters.Add("@distance", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["distance"]));
                            cmd.Parameters.Add("@msgidstr", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["idstr"]));
                            cmd.Parameters.Add("@msgtext", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["text"]);

                            string inReplyToStatusId = Convert.ToString(result["in_reply_to_status_id"]);
                            if (inReplyToStatusId.Any())
                            {
                                cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToStatusId);
                            }
                            else
                            {
                                cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value =
                                    DBNull.Value;
                            }

                            string inReplyToUserId = Convert.ToString(result["in_reply_to_user_id"]);

                            if (inReplyToUserId != null && inReplyToUserId.Any())
                            {
                                cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToUserId);
                            }
                            else
                            {
                                cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value =
                                    DBNull.Value;
                            }

                            cmd.Parameters.Add("@msgin_reply_to_screen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["in_reply_to_screen_name"]);

                            string msgfavorited = Convert.ToString(result["favorited"]);
                            cmd.Parameters.Add("@msgfavorited", System.Data.SqlDbType.Int).Value = msgfavorited.ToLower() == "true" ? 1 : 0;


                            cmd.Parameters.Add("@userscreen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["screen_name"]);

                            cmd.Parameters.Add("@userprovince", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["province"]));

                            cmd.Parameters.Add("@usercity", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["city"]));
                            cmd.Parameters.Add("@userlocation", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["location"]);

                            cmd.Parameters.Add("@userfollowers_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["followers_count"]));
                            cmd.Parameters.Add("@userfriends_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["friends_count"]));
                            cmd.Parameters.Add("@userstatuses_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["statuses_count"]));
                            cmd.Parameters.Add("@userfavourites_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["favourites_count"]));


                            string usercreatedAt = result["user"]["created_at"];
                            myDate = DateTime.ParseExact(usercreatedAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                System.Globalization.CultureInfo.InvariantCulture);

                            cmd.Parameters.Add("@usercreated_at", System.Data.SqlDbType.DateTime).Value =
                                myDate.ToUniversalTime();

                            string verified = Convert.ToString(result["user"]["verified"]);
                            cmd.Parameters.Add("@userverified", System.Data.SqlDbType.Int).Value = verified.ToLower() == "true" ? 1 : 0;

                            string userbiFollowersCount = Convert.ToString(result["user"]["bi_followers_count"]);
                            if (userbiFollowersCount.Any())
                            {
                                cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value =
                                    int.Parse(userbiFollowersCount);
                            }
                            else
                            {
                                cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value = DBNull.Value;
                            }

                            cmd.Parameters.Add("@userlang", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["user"]["lang"]);

                            string mblogid = Convert.ToString(result["user"]["client_mblogid"]);
                            if (mblogid != null && mblogid.Any())
                            {
                                cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value = mblogid;
                            }
                            else
                            {
                                cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value =
                                    DBNull.Value;
                            }
                            ////cmd.Parameters.AddWithValue("@userdescription", result["user"]["description"]);

                            written += cmd.ExecuteNonQuery();
                            cmd.Parameters.Clear();
                        }

                    }
                }
            }

            return written;
        }

        public static string SearchPOIsByGeoToSQL(dynamic S)
        {

            var bounds = (string)S["bounds"];

            string foo = "";
            foreach (var result in S["poilist"])
            {
                string gridcode = (string)result["gridcode"]; //"gridcode": "5614709820",
                string typecode = (string)result["typecode"];//"typecode": "070500",
                string tel = (string)result["tel"];//"tel": "0311-82329588",
                string newtype = (string)result["newtype"];//"newtype": "070500",
                string xml = (string)result["xml"];//"xml": "",
                string code = (string)result["code"];//"code": "130121",
                string type = (string)result["type"];//"type": "\u751f\u6d3b\u670d\u52a1;\u7269\u6d41\u901f\u9012;\u7269\u6d41\u901f\u9012",
                string imageid = (string)result["imageid"];//"imageid": "",
                string url = (string)result["url"];//"url": "url",
                string citycode = (string)result["citycode"];//"citycode": "0311",
                string timestamp = (string)result["timestamp"];//"timestamp": "2012-09-08",
                string distance = (string)result["distance"];//"distance": "191",
                string buscode = (string)result["buscode"];//"buscode": "",
                string pguid = (string)result["pguid"];//"pguid": "B01370T3AC",
                string address = (string)result["address"];//"address": "",
                string name = (string)result["name"];//"name": "\u4e95\u9649\u53bf\u91d1\u6d69\u8fd0\u8f93\u6709\u9650\u516c\u53f8",
                string linkid = (string)result["linkid"];//"linkid": "",
                string match = (string)result["match"];//"match": "10",
                string drivedistance = (string)result["drivedistance"];//"drivedistance": "0",
                string srctype = (string)result["srctype"];//"srctype": "poi",
                string y = (string)result["y"];//"y": "37.998323",
                string x = (string)result["x"];//"x": "114.100531"

                //// DO NOT DELETE JToken code = result["code"];
                //string codevalue = "";
                //if (code is JValue)
                //{
                //    codevalue = (string)code;
                //}
                //else if (code is JArray)
                //{
                //    // can pick one, or flatten array to a string
                //    codevalue = (string)((JArray)code).First;
                //}

                //Console.WriteLine("SBounds: {3}, Gridcode: {0}, Code: {1}, Type: {2}, X: {4}, Y: {5}", gridcode, codevalue, type, bounds,x,y);
                foo = foo + "(" +
                    "'" + gridcode + "'," +
"'" + typecode + "'," +
"'" + tel + "'," +
"'" + newtype + "'," +
"'" + xml + "'," +
"'" + code + "'," +
"'" + type + "'," +
"'" + imageid + "'," +
"'" + url + "'," +
"'" + citycode + "'," +
"'" + timestamp + "'," +
"'" + distance + "'," +
"'" + buscode + "'," +
"'" + pguid + "'," +
"'" + address + "'," +
"'" + name + "'," +
"'" + linkid + "'," +
"'" + match + "'," +
"'" + drivedistance + "'," +
"'" + srctype + "'," +
"'" + y + "'," +
"'" + x + "'),";


            }
            string SQLstring = "INSERT into weibotest (" +
                "gridcode," +
                "typecode," +
                "tel," +
                "newtype," +
                "xml," +
                "code," +
                "type," +
                "imageid," +
                "url," +
                "citycode," +
                "timestamp," +
                "distance," +
                "buscode," +
                "pguid," +
                "address," +
                "name," +
                "linkid," +
                "fmatch," +
                "drivedistance," +
                "srctype," +
                "y," +
                "x" +
            ") values ";
            foo = foo.TrimEnd(',');
            SQLstring = SQLstring + foo;
            return SQLstring;
        }

        private static int MySQLCountRecords()
        {

            MySqlConnection conn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand command = conn.CreateCommand();
            int count = 0;

            // READ DATA from DB
            command.CommandText = "SELECT count(distinct msgID) FROM weibo.nearbytimeline;";
            try
            {
                conn.Open();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);

            }
            MySqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine(reader["count(distinct msgID)"].ToString());
                count = Convert.ToInt32(reader["count(distinct msgID)"]);
            }
            Console.ReadLine();

            conn.Close();

            return count;
        }

        //public static T DeserializeJSon<T>(string jsonString)
        //{
        //    DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(T));
        //    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString));
        //    T obj = (T)ser.ReadObject(stream);
        //    return obj;
        //}

        public static void toLOG(string log)
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"E:\WeiboLOG\Harvester.log", true))
            {
                DateTime time = DateTime.Now;              // Use current time
                file.WriteLine(time.ToString("yyyy-MM-dd HH:mm:ss") + " " + log);
            }
        }

        public static DateTime UNIXtimeTOreadable(int t)
        {
            DateTime dateTime = new System.DateTime(1970, 1, 1, 0, 0, 0, 0);
            dateTime = dateTime.AddSeconds(t);
            return dateTime;
        }

        public static double DateTimeToUnixTimestamp(DateTime dateTime)
        {
            return (dateTime - new DateTime(1970, 1, 1).ToLocalTime()).TotalSeconds;
        }

        public List<string>[] Select()
        {
            string query = "SELECT * FROM tableinfo";

            //Create a list to store the result
            List<string>[] list = new List<string>[3];
            list[0] = new List<string>();
            list[1] = new List<string>();
            list[2] = new List<string>();

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            try
            {
                //INSERT
                MSQconn.Open();
                MSQcommand.CommandText = query;
                MySqlDataReader dataReader = MSQcommand.ExecuteReader();
                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    list[0].Add(dataReader["id"] + "");
                    list[1].Add(dataReader["name"] + "");
                    list[2].Add(dataReader["age"] + "");
                }//close Data Reader

                dataReader.Close();
                MSQconn.Close();

            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);

            }

            return list;



        }
    }

    class SQLServer
    {
        static public lists.Chexagons<int, int, int, int, double, double, double> GetHexagonsFromSQL(int p)
        {

            //LIST
            var HexagonList = new lists.Chexagons<int, int, int, int, double, double, double> { };

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //READ
            try
            {
                SqlDataReader myReader = null;
                SqlCommand myCommand = new SqlCommand("select top " + p.ToString() + " [RandomID],[TimesHarvested],[TotalCollected],[TotalInserted],[LAT],[LON],[ratio] from [weibo].[dbo].[HEXAGONPOINTS] Where [taken]=0 AND [MainlandChina]=1 ORDER BY NEWID();",
                                                         myConnection);
                myReader = myCommand.ExecuteReader();
                while (myReader.Read())
                {
                    //Console.Write(myReader["RandomID"].ToString() + "\t");
                    //Console.Write(myReader["TimesHarvested"].ToString() + "\t");
                    //Console.Write(myReader["TotalCollected"].ToString() + "\t");
                    //Console.Write(myReader["TotalInserted"].ToString() + "\t");
                    //Console.Write(myReader["LAT"].ToString() + "\t");
                    //Console.Write(myReader["LON"].ToString() + "\t");
                    //Console.Write(myReader["ratio"].ToString() + "\n");

                    HexagonList.Add(Convert.ToInt32(myReader["RandomID"]),          //Item1
                                    Convert.ToInt32(myReader["TimesHarvested"]),    //Item2
                                    Convert.ToInt32(myReader["TotalCollected"]),    //Item3
                                    Convert.ToInt32(myReader["TotalInserted"]),     //Item4
                                    Convert.ToDouble(myReader["LAT"]),              //Item5
                                    Convert.ToDouble(myReader["LON"]),              //Item6
                                    Convert.ToDouble(myReader["ratio"]));           //Item7
                }
                myReader.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            myConnection.Close();
            return HexagonList;
        }

        static public void SetHexagonsAsTakenSQL(lists.Chexagons<int, int, int, int, double, double, double> L)
        {


            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //UPDATE
            try
            {

                string s = "update [weibo].[dbo].[HEXAGONPOINTS] set [taken]=1 WHERE [RandomID]IN(";
                for (int i = 0; i < L.Count(); i++)
                {
                    s = s + L[i].Item1.ToString() + ", ";
                }
                s = s.Substring(0, s.Length - 2);
                s = s + ");";

                //Console.WriteLine(s);
                SqlCommand myCommand = new SqlCommand(s, myConnection);
                myCommand.ExecuteReader();



            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            myConnection.Close();

        }

        static public lists.AppKeysAndSecrets<int, string, string, string, string> GetAppKeysFromSQL()
        {
            //LIST
            var HarvesterList = new lists.AppKeysAndSecrets<int, string, string, string, string> { };

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }


            try
            {
                //SQL Query
                SqlDataReader myReader = null;
                SqlCommand myCommand = new SqlCommand("select top 10 [Harvester_ID],[Harvester_AppKey],[Harvester_AppSecret],[Harvester_WeiboLogin],[Harvester_WeiboPassword] from [weibo].[dbo].[APPKEYS] Where [Harvester_Status]='inactive' ORDER BY NEWID();",
                                                         myConnection);
                myReader = myCommand.ExecuteReader();
                //READ the data and store them in the list
                while (myReader.Read())
                {
                    HarvesterList.Add((int)myReader["Harvester_ID"],
                                   (string)myReader["Harvester_AppKey"],
                                   (string)myReader["Harvester_AppSecret"],
                                   (string)myReader["Harvester_WeiboLogin"],
                                   (string)myReader["Harvester_WeiboPassword"]);

                }//close Data Reader
                myReader.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            myConnection.Close();
            return HarvesterList;

        }

        static public void SetAppKeysAsTakenSQL(lists.AppKeysAndSecrets<int, string, string, string, string> A)
        {
            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }



            //UPDATE
            try
            {

                string s = "update [weibo].[dbo].[APPKEYS] set [Harvester_Status]='active' WHERE [Harvester_ID]IN(";
                for (int i = 0; i < A.Count(); i++)
                {
                    s = s + A[i].Item1.ToString() + ", ";
                }
                s = s.Substring(0, s.Length - 2);
                s = s + ");";

                //Console.WriteLine(s);
                SqlCommand myCommand = new SqlCommand(s, myConnection);
                myCommand.ExecuteReader();



            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            myConnection.Close();

        }

        static public Tuple<int, int> NearbyTimelineToSQL(dynamic s, string sid, string fid, int fgid)
        {

            int counts = 0;
            int written = 0;

            //InsertCommand
            const string insertCommand = "INSERT INTO [weibo].[dbo].[NBT2] (" +
                                         "SeasonID," +
                                         "FieldID," +
                                         "FieldGroupID," +
                                         "createdAT," +
                                         "msgID," +
                                         "msgmid," +
                                         "geoTYPE," +
                                         "geoLAT," +
                                         "geoLOG," +
                                         "userID," +
                                         "distance," +
                //InnoDB// "Point," +
                                         "msgidstr," +
                                         "msgtext," +
                                         "msgin_reply_to_status_id," +
                                         "msgin_reply_to_user_id," +
                                         "msgin_reply_to_screen_name," +
                                         "msgfavorited," +
                                         "userscreen_name," +
                                         "userprovince," +
                                         "usercity," +
                                         "userlocation," +
                                         "userfollowers_count," +
                                         "userfriends_count," +
                                         "userstatuses_count," +
                                         "userfavourites_count," +
                                         "usercreated_at," +
                                         "usergeo_enabled," +
                                         "userverified," +
                                         "userbi_followers_count," +
                                         "userlang," +
                                         "userclient_mblogid" +
                //"userdescription" +
                                         ")" +

                                         " values " +
                                         "(" +
                                         "@SID," +
                                         "@FID," +
                                         "@FGID," +
                                         "@createdAT," +
                                         "@msgid," +
                                         "@msgmid," +
                                         "@geoTYPE," +
                                         "@geoLAT," +
                                         "@geoLOG," +
                                         "@userID," +
                                         "@distance," +
                //InnoDB// "GeomFromText(@Point)," +
                                         "@msgidstr," +
                                         "@msgtext," +
                                         "@msgin_reply_to_status_id," +
                                         "@msgin_reply_to_user_id," +
                                         "@msgin_reply_to_screen_name," +
                                         "@msgfavorited," +
                                         "@userscreen_name," +
                                         "@userprovince," +
                                         "@usercity," +
                                         "@userlocation," +
                                         "@userfollowers_count," +
                                         "@userfriends_count," +
                                         "@userstatuses_count," +
                                         "@userfavourites_count," +
                                         "@usercreated_at," +
                                         "1," +
                                         "@userverified," +
                                         "@userbi_followers_count," +
                                         "@userlang," +
                                         "@userclient_mblogid" +
                //"@userdescription" +
                                         ")";
            try
            {
                using (var tsc = new TransactionScope()) //Not sure if it is implemented properly!
                {
                    //Console.WriteLine("In TransacrtionScope");
                    using (var conn1 = new SqlConnection(Properties.Settings.Default.MSSQL))
                    {
                        conn1.Open();

                        

                        //Console.Write(" conn1 Open ");
                        //Console.WriteLine(s);
                        foreach (var result in s["statuses"])
                        {
                            SqlCommand cmd = new SqlCommand(insertCommand, conn1);
                            counts = counts + 1;
                            //Console.WriteLine("Status nr. " + counts.ToString());
                            var geoEnabled = (string)result["user"]["geo_enabled"];
                            if (String.Equals(geoEnabled, "true", StringComparison.InvariantCultureIgnoreCase))
                            {
                                //cmd.Parameters.Add
                                cmd.Parameters.Add("@SID", System.Data.SqlDbType.Int).Value = sid;
                                cmd.Parameters.Add("@FID", System.Data.SqlDbType.Int).Value = fid;
                                cmd.Parameters.Add("@FGID", System.Data.SqlDbType.Int).Value = fgid;

                                var createdAt = (string)result["created_at"];
                                DateTime myDate = DateTime.ParseExact(createdAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                    System.Globalization.CultureInfo.InvariantCulture);
                                //myDate.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                                cmd.Parameters.Add("@createdAT", System.Data.SqlDbType.DateTime).Value =
                                    myDate.ToUniversalTime();


                                cmd.Parameters.Add("@msgid", System.Data.SqlDbType.BigInt).Value =
                                    long.Parse(Convert.ToString(result["id"]));
                                //Console.WriteLine(result["id"]);


                                cmd.Parameters.Add("@msgmid", System.Data.SqlDbType.BigInt).Value =
                                    long.Parse(Convert.ToString(result["mid"]));


                                cmd.Parameters.Add("@geoTYPE", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["geo"]["type"]);
                                cmd.Parameters.Add("@geoLAT", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][0]));
                                cmd.Parameters.Add("@geoLOG", System.Data.SqlDbType.Float).Value = double.Parse(Convert.ToString(result["geo"]["coordinates"][1]));

                                //cmd.Parameters.AddWithValue("@usergeo_enabled", 1);

                                cmd.Parameters.Add("@userID", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["user"]["id"]));
                                cmd.Parameters.Add("@distance", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["distance"]));
                                cmd.Parameters.Add("@msgidstr", System.Data.SqlDbType.BigInt).Value = long.Parse(Convert.ToString(result["idstr"]));
                                cmd.Parameters.Add("@msgtext", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["text"]);

                                string inReplyToStatusId = Convert.ToString(result["in_reply_to_status_id"]);
                                if (inReplyToStatusId.Any())
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToStatusId);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_status_id", System.Data.SqlDbType.BigInt).Value =
                                        DBNull.Value;
                                }

                                string inReplyToUserId = Convert.ToString(result["in_reply_to_user_id"]);

                                if (inReplyToUserId != null && inReplyToUserId.Any())
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value = long.Parse(inReplyToUserId);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@msgin_reply_to_user_id", System.Data.SqlDbType.BigInt).Value =
                                        DBNull.Value;
                                }

                                cmd.Parameters.Add("@msgin_reply_to_screen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["in_reply_to_screen_name"]);

                                string msgfavorited = Convert.ToString(result["favorited"]);
                                cmd.Parameters.Add("@msgfavorited", System.Data.SqlDbType.Int).Value = msgfavorited.ToLower() == "true" ? 1 : 0;


                                cmd.Parameters.Add("@userscreen_name", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["screen_name"]);

                                cmd.Parameters.Add("@userprovince", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["province"]));

                                cmd.Parameters.Add("@usercity", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["city"]));
                                cmd.Parameters.Add("@userlocation", System.Data.SqlDbType.NVarChar).Value = Convert.ToString(result["user"]["location"]);

                                cmd.Parameters.Add("@userfollowers_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["followers_count"]));
                                cmd.Parameters.Add("@userfriends_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["friends_count"]));
                                cmd.Parameters.Add("@userstatuses_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["statuses_count"]));
                                cmd.Parameters.Add("@userfavourites_count", System.Data.SqlDbType.Int).Value = int.Parse(Convert.ToString(result["user"]["favourites_count"]));


                                string usercreatedAt = result["user"]["created_at"];
                                myDate = DateTime.ParseExact(usercreatedAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                                    System.Globalization.CultureInfo.InvariantCulture);

                                cmd.Parameters.Add("@usercreated_at", System.Data.SqlDbType.DateTime).Value =
                                    myDate.ToUniversalTime();

                                string verified = Convert.ToString(result["user"]["verified"]);
                                cmd.Parameters.Add("@userverified", System.Data.SqlDbType.Int).Value = verified.ToLower() == "true" ? 1 : 0;

                                string userbiFollowersCount = Convert.ToString(result["user"]["bi_followers_count"]);
                                if (userbiFollowersCount.Any())
                                {
                                    cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value =
                                        int.Parse(userbiFollowersCount);
                                }
                                else
                                {
                                    cmd.Parameters.Add("@userbi_followers_count", System.Data.SqlDbType.Int).Value = DBNull.Value;
                                }

                                cmd.Parameters.Add("@userlang", System.Data.SqlDbType.VarChar).Value = Convert.ToString(result["user"]["lang"]);

                                string mblogid = Convert.ToString(result["user"]["client_mblogid"]);
                                if (mblogid != null && mblogid.Any())
                                {
                                    cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value = mblogid;
                                }
                                else
                                {
                                    cmd.Parameters.Add("@userclient_mblogid", System.Data.SqlDbType.NVarChar).Value =
                                        DBNull.Value;
                                }
                                ////cmd.Parameters.AddWithValue("@userdescription", result["user"]["description"]);


                            }//If geoenabled
                            //Console.Write(counts.ToString());
                            try
                            {
                                //SQL throws a VIOLATION not the number of rows affected (not so nice)
                                written += cmd.ExecuteNonQuery();

                            }
                            catch (Exception ex)
                            {
                                //Console.WriteLine(ex);
                            }

                        }//For each Loop
                        if (written >= 1 && written <= 50)
                        {
                            //good
                        }
                        else {
                            written = 0;
                        }
                        //Console.Write("written: " + written.ToString());
                        //cmd.Parameters.Clear();
                        //conn1.Close();
                    }//SQL connection
                    tsc.Complete();

                }//TransactionScope
            }
            catch (TransactionAbortedException ex)
            {
                Console.WriteLine("TransactionAbortedException Message: {0}", ex.Message);
            }
            catch (ApplicationException ex)
            {
                Console.WriteLine("ApplicationException Message: {0}", ex.Message);
            }
            var res = new Tuple<int, int>(counts, written);
            return res;
        }

        static public Tuple<int, int> NearbyTimelineToSQLnonparam(dynamic s, string sid, string fid, int fgid)
        {

            int counts = 0;
            int written = 0;
          
            //CONNECT
            


            string insertCommand = "INSERT INTO [weibo].[dbo].[NBT] (" +
                                         "SeasonID," +
                                         "FieldID," +
                                         "FieldGroupID," +
                                         "createdAT," +
                                         "msgID," +
                                         "msgmid," +
                                         "geoTYPE," +
                                         "geoLAT," +
                                         "geoLOG," +
                                         "userID," +
                                         "distance," +
                //InnoDB// "Point," +
                                         "msgidstr," +
                                         "msgtext," +
                                         "msgin_reply_to_status_id," +
                                         "msgin_reply_to_user_id," +
                                         "msgin_reply_to_screen_name," +
                                         "msgfavorited," +
                                         "userscreen_name," +
                                         "userprovince," +
                                         "usercity," +
                                         "userlocation," +
                                         "userfollowers_count," +
                                         "userfriends_count," +
                                         "userstatuses_count," +
                                         "userfavourites_count," +
                                         "usercreated_at," +
                                         "usergeo_enabled," +
                                         "userverified," +
                                         "userbi_followers_count," +
                                         "userlang," +
                                         "userclient_mblogid" +
                //"userdescription" +
                                         ")" +

                                         " values " +
                                         "(";

            try
            {

                foreach (var result in s["statuses"])
                {
                    counts = counts + 1;
                    insertCommand = insertCommand + sid.ToString() + ", ";
                    insertCommand = insertCommand + fid.ToString() + ", ";
                    insertCommand = insertCommand + fgid.ToString() + ", ";
                    insertCommand = insertCommand + fgid.ToString() + ", ";
                    var createdAt = (string)result["created_at"];
                    DateTime myDate = DateTime.ParseExact(createdAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                        System.Globalization.CultureInfo.InvariantCulture);
                    insertCommand = insertCommand + myDate.ToUniversalTime().ToString() + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["id"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["mid"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["geo"]["type"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["geo"]["coordinates"][0]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["geo"]["coordinates"][1]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["id"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["distance"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["idstr"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["text"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["in_reply_to_status_id"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["in_reply_to_user_id"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["in_reply_to_screen_name"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["favorited"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["screen_name"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["province"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["city"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["location"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["followers_count"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["friends_count"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["statuses_count"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["favourites_count"]) + ", ";
                    string usercreatedAt = result["user"]["created_at"];
                    myDate = DateTime.ParseExact(usercreatedAt.Substring(4), "MMM dd HH:mm:ss zzzzz yyyy",
                        System.Globalization.CultureInfo.InvariantCulture);
                    insertCommand = insertCommand + myDate.ToUniversalTime();
                    insertCommand = insertCommand + myDate.ToUniversalTime().ToString() + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["verified"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["bi_followers_count"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["lang"]) + ", ";
                    insertCommand = insertCommand + Convert.ToString(result["user"]["client_mblogid"]) + ", ";
                    insertCommand = insertCommand.Substring(0, insertCommand.Length - 2);
                    insertCommand = insertCommand + "; ";
                }



                insertCommand = insertCommand.Substring(0, insertCommand.Length - 2);
                insertCommand = insertCommand + ");";

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            Console.Write("HERE");

            Console.WriteLine("/n/n/n" + insertCommand + "/n/n/n/n");

            



            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
var cmd = new SqlCommand(insertCommand, myConnection);

            try
            {
                written = cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            cmd.Parameters.Clear();


            var res = new Tuple<int, int>(counts, written);
            return res;
        }
    }

    class WeiboMySQL
    {
        static public lists.AppKeysAndSecrets<int, string, string, string, string> GetHarvestersFromDB()
        {
            string query = "GetAppKeysAndSecrets";
            var HarvesterList = new lists.AppKeysAndSecrets<int, string, string, string, string> { };


            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Connection = MSQconn;


            try
            {
                //INSERT
                MSQconn.Open();
                MSQcommand.CommandText = query;
                MySqlDataReader dataReader = MSQcommand.ExecuteReader();
                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    HarvesterList.Add((int)dataReader["Harvester_ID"],
                                   (string)dataReader["Harvester_AppKey"],
                                   (string)dataReader["Harvester_AppSecret"],
                                   (string)dataReader["Harvester_WeiboLogin"],
                                   (string)dataReader["Harvester_WeiboPassword"]);

                }//close Data Reader

                dataReader.Close();
                MSQconn.Close();

            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);

            }
            return HarvesterList;



        }

        static public Tuple<DateTime, DateTime, DateTime, int, int> GetSeasonFromDB()
        {
            string query = "GetNextSeason";

            //Create a variables to return
            DateTime StartTime = new DateTime();
            DateTime EndTime = new DateTime();
            DateTime CurrentTime = new DateTime();
            int GroupID = 0;
            int SeasonID = 0;
            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MySqlDataReader dataReader = MSQcommand.ExecuteReader();
                //Read the data and store them in the list. Be care full, if the result is NULL SeasonID will be 0
                while (dataReader.Read())
                {
                    //Use same column names as in MySQL DB!!!
                    StartTime = (DateTime)dataReader["Season_StartTime"];
                    EndTime = (DateTime)dataReader["Season_EndTime"];
                    CurrentTime = (DateTime)dataReader["Season_CurrentTime"];
                    GroupID = (int)dataReader["Season_FieldsGroupID"];
                    SeasonID = (int)dataReader["Season_ID"];
                }//close Data Reader
                dataReader.Close();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }
            return Tuple.Create(StartTime, EndTime, CurrentTime, GroupID, SeasonID);
        }

        static public lists.FieldCoordinates<int, double, double, int> GetFieldsFromDB()
        {
            string query = "GetGroupOfFieldsAll";

            //Create a list to store the result
            var FieldsList = new lists.FieldCoordinates<int, double, double, int> { };

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MySqlDataReader dataReader = MSQcommand.ExecuteReader();
                //Read the data and store them in the list
                while (dataReader.Read())
                {
                    //Use same column names as in MySQL DB!!!
                    FieldsList.Add((int)dataReader["Fields_ID"],
                                   (double)dataReader["Latitude"],//Center coordinates of hexagon
                                   (double)dataReader["Longitude"],
                                   (int)dataReader["Fields_GroupID"]);//Center coordinates of hexagon

                }//close Data Reader
                dataReader.Close();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }
            return FieldsList;
        }

        static public lists.Chexagons<int, int, int, int, double, double, double> GetHexagonsFromMSSQL(int p)
        {

            //LIST
            var HexagonList = new lists.Chexagons<int, int, int, int, double, double, double> { };

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //READ
            try
            {
                SqlDataReader myReader = null;
                SqlCommand myCommand = new SqlCommand("select top " + p.ToString() + " [RandomID],[TimesHarvested],[TotalCollected],[TotalInserted],[LAT],[LON],[ratio] from [weibo].[dbo].[HEXAGONPOINTS] Where [taken]=0 AND [MainlandChina]=1 ORDER BY NEWID();",
                                                         myConnection);
                myReader = myCommand.ExecuteReader();
                while (myReader.Read())
                {
                    //Console.Write(myReader["RandomID"].ToString() + "\t");
                    //Console.Write(myReader["TimesHarvested"].ToString() + "\t");
                    //Console.Write(myReader["TotalCollected"].ToString() + "\t");
                    //Console.Write(myReader["TotalInserted"].ToString() + "\t");
                    //Console.Write(myReader["LAT"].ToString() + "\t");
                    //Console.Write(myReader["LON"].ToString() + "\t");
                    //Console.Write(myReader["ratio"].ToString() + "\n");

                    HexagonList.Add(Convert.ToInt32(myReader["RandomID"]),          //Item1
                                    Convert.ToInt32(myReader["TimesHarvested"]),    //Item2
                                    Convert.ToInt32(myReader["TotalCollected"]),    //Item3
                                    Convert.ToInt32(myReader["TotalInserted"]),     //Item4
                                    Convert.ToDouble(myReader["LAT"]),              //Item5
                                    Convert.ToDouble(myReader["LON"]),              //Item6
                                    Convert.ToDouble(myReader["ratio"]));           //Item7
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            myConnection.Close();
            return HexagonList;
        }

        static public void MarkHexagonsFromMSSQL(lists.Chexagons<int, int, int, int, double, double, double> L)
        {


            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //UPDATE

            try
            {

                string s = "update [weibo].[dbo].[HEXAGONPOINTS] set [taken]=1 WHERE [RandomID]IN(";
                for (int i = 0; i < L.Count(); i++)
                {
                    s = s + L[i].Item1.ToString() + ", ";
                }
                s = s.Substring(0, s.Length - 2);
                s = s + ");";

                //Console.WriteLine(s);
                SqlCommand myCommand = new SqlCommand(s, myConnection);
                myCommand.ExecuteReader();



            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }



            myConnection.Close();

        }

        static public void UpdateHexagonsFromMSSQL(lists.Chexagons<int, int, int, int, double, double, double> fulllist)
        {

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }


            int y = 1;
            int g = fulllist.Count();
            int interval = (int)(g / 10);
            Console.Write("\nStart updating MSSQL: 1");
            //READ
            for (int i = 0; i < fulllist.Count(); i++)
            {
                if (i >= (interval * y))
                {
                    Console.Write(".." + (10 * y).ToString());
                    y = y + 1;
                }

                try
                {
                    //Console.WriteLine("This list has entries: " + fulllist.Count().ToString());
                    string s = "";

                    string ID = fulllist[i].Item1.ToString();
                    string TimesHarvested = fulllist[i].Item2.ToString();
                    string TotalCollected = fulllist[i].Item3.ToString();
                    string TotalInserted = fulllist[i].Item4.ToString();
                    string ratio = fulllist[i].Item7.ToString();
                    s = s + "update [weibo].[dbo].[HEXAGONPOINTS] set [TimesHarvested]=" + TimesHarvested + ",[TotalCollected]=" + TotalCollected + ",[TotalInserted]=" + TotalInserted + ",[ratio]=" + ratio + " WHERE [RandomID]=" + ID + ";";


                    //Console.WriteLine(s);
                    SqlCommand myCommand = new SqlCommand(s, myConnection);
                    myCommand.ExecuteNonQuery();


                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }

            }

            myConnection.Close();

        }

        static public void UpdateSingleHexagonFromMSSQL(int RandomID, int TimesHarvested, int TotalCollected, int TotalInserted, double ratio)
        {

            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            Console.Write("MSSQL ratio: " + String.Format("{0,5:#0.00}", ratio));

            try
            {
                //Console.WriteLine("This list has entries: " + fulllist.Count().ToString());
                string s = "update [weibo].[dbo].[HEXAGONPOINTS] "
                            + "set [TimesHarvested]=" + TimesHarvested.ToString()
                               + ",[TotalCollected]=" + TotalCollected.ToString()
                               + ",[TotalInserted]=" + TotalInserted.ToString()
                               + ",[ratio]=" + String.Format("{0,5:#0.00}", ratio)
                               + " WHERE [RandomID]=" + RandomID.ToString()
                               + ";";


                //Console.WriteLine(s);
                SqlCommand myCommand = new SqlCommand(s, myConnection);
                myCommand.ExecuteNonQuery();


            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            myConnection.Close();

        }

        static public void SetAppKeysAndSecrestStatus(int HarvesterID, string Status, int Season)
        {
            string query = "SetAppKeysAndSecretsStatus";

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Parameters.AddWithValue("?HarvesterID", HarvesterID);
            MSQcommand.Parameters["?HarvesterID"].Direction = ParameterDirection.Input;
            MSQcommand.Parameters.AddWithValue("?statusM", Status);
            MSQcommand.Parameters["?statusM"].Direction = ParameterDirection.Input;
            MSQcommand.Parameters.AddWithValue("?season", Season);
            MSQcommand.Parameters["?season"].Direction = ParameterDirection.Input;

            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MSQcommand.ExecuteReader();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }

        }

        static public void SetSeasonStatus(int SeasonID, DateTime T, string Status)
        {
            string query = "SetSeasonStatus";

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Parameters.AddWithValue("?ID", SeasonID);
            MSQcommand.Parameters["?ID"].Direction = ParameterDirection.Input;
            MSQcommand.Parameters.AddWithValue("?currentTime", T);
            MSQcommand.Parameters["?currentTime"].Direction = ParameterDirection.Input;
            MSQcommand.Parameters.AddWithValue("?statusM", Status);
            MSQcommand.Parameters["?statusM"].Direction = ParameterDirection.Input;

            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MSQcommand.ExecuteReader();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }

        }

        static public void SetFieldStatus(int SeasonID, string Status)
        {
            string query = "SetFieldStatus";

            //MySQL connection
            MySqlConnection MSQconn = new MySqlConnection(Properties.Settings.Default.connString);
            MySqlCommand MSQcommand = MSQconn.CreateCommand();

            MSQcommand.CommandText = query;
            MSQcommand.CommandType = CommandType.StoredProcedure;
            MSQcommand.Parameters.AddWithValue("?ID", SeasonID);
            MSQcommand.Parameters["?ID"].Direction = ParameterDirection.Input;
            MSQcommand.Parameters.AddWithValue("?statusM", Status);
            MSQcommand.Parameters["?statusM"].Direction = ParameterDirection.Input;

            MSQcommand.Connection = MSQconn;

            try
            {
                //CALL
                MSQconn.Open();
                MSQcommand.ExecuteReader();
                MSQconn.Close();
            }
            catch (Exception ex)
            {
                Console.Write(ex.ToString());
                Console.WriteLine(ex.Message);
            }

        }
    }


    //Different Lists
    class lists
    {
        //Tuple from MySQL DB
        public class FieldCoordinates<T1, T2, T3, T4> : List<Tuple<T1, T2, T3, T4>>
        {
            public void Add(T1 item, T2 item2, T3 item3, T4 item4)
            {
                Add(new Tuple<T1, T2, T3, T4>(item, item2, item3, item4));
            }
        }

        //Tuple from MySQL DB
        public class AppKeysAndSecrets<T1, T2, T3, T4, T5> : List<Tuple<T1, T2, T3, T4, T5>>
        {
            public void Add(T1 item, T2 item2, T3 item3, T4 item4, T5 item5)
            {
                Add(new Tuple<T1, T2, T3, T4, T5>(item, item2, item3, item4, item5));
            }
        }
        //Tuple from MSSQL DB
        public class Chexagons<T1, T2, T3, T4, T5, T6, T7> : List<Tuple<T1, T2, T3, T4, T5, T6, T7>>
        {
            public void Add(T1 item, T2 item2, T3 item3, T4 item4, T5 item5, T6 item6, T7 item7)
            {
                Add(new Tuple<T1, T2, T3, T4, T5, T6, T7>(item, item2, item3, item4, item5, item6, item7));
            }
        }

    }



}
