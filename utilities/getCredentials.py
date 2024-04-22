def get_cred(file, env_vars):
    env_vals = {}
    with open(file, "r") as f:
        for line in f:
            for k, v in env_vars.items():
                if line.startswith(f"export {v}"):
                    env_vals[k] = line.split("=")[1].replace("\n", "")

    return env_vals
