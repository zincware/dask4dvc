from zntrack import Node, zn


class InputToOutput(Node):
    outputs = zn.outs()
    inputs = zn.params()

    def run(self):
        self.outputs = self.inputs
