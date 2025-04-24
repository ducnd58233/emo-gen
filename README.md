# <center>Project: Emoji Generator</center>
1. Clone the service
- Notes:
  * `<field>`:
    * backend
    * frontend
    * ai
    * data
    * devops
  * `<service>`: check inside the <field> directory, e.g landside

- Using `sparse-checkout` to avoid cloning full repository
- Choose the <field> with the <service> you want to clone
```sh
git clone --no-checkout git@github.com:ducnd58233/emo-gen.git
cd emo-gen
git sparse-checkout set <field>/<service>
git sparse-checkout list
git checkout
```
