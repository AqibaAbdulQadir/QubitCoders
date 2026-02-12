# QubitCoders - LeetCode Profile Analyzer

Last Updated: February 12, 2026
---
## Overview
QubitCoders is a web app designed for **CIS students at NEDUET** and aspiring coders to improve their problem-solving skills by analyzing their LeetCode profiles. The app provides personalized advice on how to improve based on contest ratings, the number of problems solved, and expertise level.

### Features
- **Ranking**: View the rankings of CIS students in the community.
- **Classifier**: Get your LeetCode profile classified based on your skills (Beginner, Intermediate, Advanced).
- **Recommendations**: Get personalized advice based on your LeetCode profile, including solved problems in Easy, Medium, and Hard categories as well as your contest rating.

---

## Table of Contents
1. [Installation](#installation)
2. [Usage](#usage)
3. [App Features](#app-features)
4. [Technologies Used](#technologies-used)
5. [Model Integration](#model-integration)
6. [Deployment](#deployment)
7. [Contributors](#contributor)

---

## Installation

To run the **QubitCoders** app locally, follow these steps:

### Prerequisites
- Python >= 3.7
- Streamlit
- Gemini

### Clone the repository
```bash
git clone https://github.com/yourusername/qubitcoders.git
cd qubitcoders
```

### Install dependencies
```bash
pip install -r requirements.txt
```
---
## Usage

1. Launch the app:
```bash
streamlit run app.py
```

2. Navigate through the app:
- On the Welcome Page, learn more about the QubitCoders community and its purpose.
- Ranking: View the leaderboard for CIS students in the department.
- Classifier: Enter your LeetCode username to classify your expertise level and receive recommendations.

---
## App Features

### Ranking
View the leaderboard to see how you rank compared to other CIS students at NEDUET.

### Classifier
Enter your LeetCode profile to classify your expertise level:
- Beginner
- Beginner-intermediate
- Intermediate-advanced
- Advanced

---
## Technologies Used

- Streamlit: Web framework used to build the app.
- Python: Programming language.
- HuggingFace: Used for integrating LLaMA-2 for personalized coding advice. 
- pandas: For handling data processing and displaying dataframes.
- scikit-fuzzy: Used for expertise classification based on fuzzy logic.
- GitHub: For version control and project management.

---
## Model Integration

### Expertise Classifier
The app uses scikit-fuzzy to classify users into four expertise levels based on their contest ratings and the number of problems solved (easy, medium, hard).

### Recommendations via Gemini API
We integrated the Gemini model to generate personalized advice based on the user’s LeetCode profile. The input includes:

- Expertise level
- Contest rating
- Number of solved easy, medium, and hard problems


## Deployment
The QubitCoders app is deployed on Streamlit Cloud. To view the live version, visit [https://qubitcoders.streamlit.app/](https://qubitcoders.streamlit.app/).

---
## Contributor

- Aqiba Abdul Qadir: Creator and Developer
