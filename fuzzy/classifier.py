import os
import pickle
import requests
from skfuzzy import control as ctrl
import google.generativeai as genai
from streamlit import cache_resource, secrets


@cache_resource
def load_fuzz_model():
    base_path = os.path.abspath(os.path.dirname(__file__))
    pkl_path = os.path.join(base_path, "fuzzy_system_model.pkl")
    with open(pkl_path, 'rb') as f:
        loaded_system = pickle.load(f)
    return loaded_system

@cache_resource
def load_llm_model():
    os.environ["GEMINI_API_KEY"] = secrets["genai"]["api_key"]
    genai.configure(api_key=os.environ["GEMINI_API_KEY"])

    generation_config = {
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 8192,
    }

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        generation_config=generation_config,
    )

    return model


def fetch_api_data(username):
    base = "https://leetcode-api-pied.vercel.app"
    try:
        r = requests.get(f'{base}/user/{username}')
        data = r.json()
        subs = data["submitStats"]["acSubmissionNum"]
        # submission_data = (data['easySolved'], data['mediumSolved'], data['hardSolved'])
        submission_data = (subs[1]["count"], subs[2]["count"], subs[3]["count"])
        # print(submission_data)
        res = 'Successful!'

    except KeyError:
        submission_data = None
        res = 'User not found!'

    except:
        submission_data = None
        res = 'Limit exceeded!'

    try:
        r = requests.get(f'{base}/user/{username}/contests')
        data = r.json()
        contest_data = (data['userContestRanking']['rating'],)
        # print(contest_data)

    except KeyError:
        contest_data = (0,)

    except:
        contest_data = (0,)

    return [contest_data, submission_data, res]


def get_user_expertise(contest_data, submission_data):
    loaded_system = load_fuzz_model()
    expertise_sim = ctrl.ControlSystemSimulation(loaded_system)

    expertise_sim.input['contest_rating'] = contest_data[0]
    expertise_sim.input['easies'] = submission_data[0]
    expertise_sim.input['mediums'] = submission_data[1]
    expertise_sim.input['hards'] = submission_data[2]

    expertise_sim.compute()
    expertise_level = round(expertise_sim.output['expertise'], 2)

    if 0 <= expertise_level < 3.5: expertise_name = "beginner"
    elif 3.5 < expertise_level < 6.2: expertise_name = "beginner-intermediate"
    elif 6.2 <= expertise_level < 8.2: expertise_name = "intermediate-advanced"
    else: expertise_name = "advanced"

    return expertise_level, expertise_name


def get_user_analysis(expertise_name, contest_data, submission_data):

    model = load_llm_model()
    contest_rating = contest_data[0]
    easies, mediums, hards = submission_data

    response = model.generate_content([
        f'''You need to give advice to a user.
        The user is a competitive programmer providing you with their data as follows:
        \nHard questions solved: {hards}
        \nMedium questions solved: {mediums}
        \nEasy questions solved: {easies}
        \nThe user's expertise level is: {expertise_name}
        \nBased on this information, give the user a personalized recommendation to help them improve in 
        competitive programming pointing towards their current status.
        \nFocus on areas they should work on, such as
        \n- Which type of problems (Medium, Hard) they should solve more. If they are beginners, focus on mediums else hards
        \n- What kind of data structures and algorithms they should practice more based on their expertise.
        Contest rating above 2000 is considered advanced.
        The contest rating of the user is {contest_rating}. 
        \n- Based on this, advise whether they should participate more in contests.
        
        \n- Then give any specific advice to boost their problem-solving skills.
        \n- Any routine habit like sleeping, practice, exercise to enhance their competitive programming skills.
        \nKeep the recommendation within 250 words. 
        Your recommendation should be fun, engaging, motivational, and actionable. Begin with Assalam u alaikum!''',
    ])
    return response.text


def get_response(username):
    contest_data, submission_data, res = fetch_api_data(username)
    if res == 'User not found!': return None, None, None, None, 'User does not exist!'
    elif res == 'Limit exceeded!': return None, None, None, None, 'Rate limit exceeded! Please try again in 5 minutes.'
    expertise_level, expertise_name = get_user_expertise(contest_data, submission_data)
    recom = get_user_analysis(expertise_level, expertise_name, submission_data)
    return expertise_level, expertise_name, recom, contest_data, submission_data, 'Successful!'



