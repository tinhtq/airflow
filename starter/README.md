Here’s the content from the image converted to markdown format:

---

## Task Instructions

### 1. Rewrite the DAG Using the TaskFlow API

Your new DAG should:
- Use the `@dag` decorator to define the DAG.
- Use the `@task` decorator for each task.
- Utilize the TaskFlow API's automatic XCom feature for passing the random number between tasks.
- Maintain the same functionality as the original DAG.

---

### 2. Functionality Requirements

Ensure that your new DAG:
- Generates a random number between 1 and 100.
- Passes this number to the next task without explicitly using XCom methods.
- Checks whether the number is even or odd.
- Prints appropriate messages at each step.

---

### 3. Test Your DAG

Make sure it works as expected:
- Add the DAG to the `dags/` folder (wait until it shows up on the UI, up to 5 minutes).
- Unpause the DAG by turning on the toggle next to its name.
- Check the logs of the `check_even_odd` task.

---