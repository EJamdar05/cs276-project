import json
import pandas as pd
data_list = []

stance_labels = {
    "pro-Russia" : [
        "This statement is in favour of Russia",
        "This statement is in favour of war",
        "This statement is in favour of military conflict"
    ],
    "pro-Ukraine" : [
        "This statement is against Russia",
        "This statement is in favour of Ukraine",
        "This statement is against war",
        "This statement is against military conflict"
    ]
}

print("Opening file")
with open("dataset_zenodo.jsonl", "r") as file:
    print("Starting process.......")
    for entry in file:
        entry = entry.strip()
        if not entry:
            continue
        try:
            data = json.loads(entry)
            selected_data = {
                "tweet_id" : data["tweet_id"],
                "lang" : data["lang"],
                "country" : data["country"],
                "verified" : data["verified"],
                "sentiment_neutral": data.get("sentiment", {}).get("neutral", 0)        
            }

            entail_prob_total = 0
            contra_prob_total = 0
            stance_list = data.get("stance", [])
            stance_total = len(stance_list)

            stance_out = "unsure"
            for stance in data.get("stance", []):
                hypothesis = stance["hypothesis"]
                entail_prob = float(stance["entail_prob"])

                if hypothesis in stance_labels["pro-Russia"] and entail_prob >= 0.9:
                    stance_out = "pro-Russia"
                    break
                elif hypothesis in stance_labels["pro-Ukraine"] and entail_prob >= 0.9:
                    stance_out = "pro-Ukraine"
                    break
                
            for stance in stance_list:
                entail_prob_total += float(stance["entail_prob"])
                contra_prob_total += float(stance["contra_prob"])

            selected_data["entail_prob"] = round((entail_prob_total / stance_total), 2) if stance_total else 0
            selected_data["contra_prob"] = round((contra_prob_total / stance_total), 2) if stance_total else 0

            selected_data["stance"] = stance_out

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Skipping line {e}")
        data_list.append(selected_data)

print("Writing to output.csv")
df = pd.DataFrame(data_list)
df.to_csv("output.csv", index = False)
print("Done!")





