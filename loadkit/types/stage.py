from archivekit import Resource


class Stage(Resource):
    """ Stages are the intermediate document types produced by
    a processing pipeline. """

    GROUP = 'stages'

    def __repr__(self):
        return '<Stage(%r)>' % self.name
