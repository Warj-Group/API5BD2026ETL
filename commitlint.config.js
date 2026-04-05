module.exports = {
  parserPreset: {
    parserOpts: {
      headerPattern: /^(feat|fix|doc|style|refactor|test|chore|ci)\/(WARJ-\d+|main|sprint-\d+): (.+)$/,
      headerCorrespondence: ['type', 'scope', 'subject']
    }
  },
  rules: {
    'type-empty': [2, 'never'],
    'subject-empty': [2, 'never'],
    'type-enum': [2, 'always', ['feat', 'fix', 'doc', 'style', 'refactor', 'test', 'chore', 'ci']]
  }
};
