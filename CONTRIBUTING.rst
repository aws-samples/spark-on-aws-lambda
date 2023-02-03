How to Contribute
=================

Contributions are welcome! First time contributor? No worry, it's alright. We've all been there. 
There are many ways to contribute to open source projects: reporting bugs,
helping with the documentation, spreading the word and of course, adding
new features and patches.

Getting Started
---------------
#. Make sure you have a GitLab account.
#. Open a `new issue`_, assuming one does not already exist.
#. Clearly describe the issue including steps to reproduce when it is a bug.

Making Changes
--------------
* Fork_ the repository on GitLab.
* Create a branch from where you want to base your work. This is usually from the ``master`` branch.
* Please avoid working directly on ``master`` branch.
* Make commits of logical units (if needed rebase your feature branch before submitting it).
* Make sure your commit messages are in the `proper format`_.
* Make sure you have added the necessary tests for your changes.
* Run all the tests to assure nothing else was accidentally broken.
* Don't forget to add yourself to AUTHORS_.

These guidelines also apply when helping with documentation.

Submitting Changes
------------------
* Push your changes to a topic branch in your fork of the repository.
* Submit a `Pull Request`_.
* Wait for maintainer feedback.

Don't know where to start?
--------------------------
There are usually several TODO comments scattered around the codebase, maybe
check them out and see if you have ideas and can help with them. Also, check
the `open issues`_ in case there's something that sparks your interest. 
In any case, other than GitLab help_ pages, you might want to check this
excellent `Effective Guide to Pull Requests`_

Finding contributions to work on
--------------------------
Looking at the existing issues is a great way to find something to contribute on. 
As our projects, by default, use the default issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), 
looking at any 'help wanted' issues is a great place to start.


Code of Conduct
--------------------------
This project has adopted the `Amazon Open Source Code of Conduct`_.
For more information see the `Code of Conduct FAQ`_ or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


Security issue notifications
--------------------------
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our `vulnerability reporting page`_. 
Please do **not** create a public issue.


Licensing
--------------------------
See the LICENSE_ file for our project's licensing. We will ask you to confirm the licensing of your contribution.


.. _`the repository`: https://gitlab.aws.dev/aws-data-lab/bookmark-utils/
.. _AUTHORS: ./AUTHORS
.. _`open issues`: https://gitlab.aws.dev/aws-data-lab/bookmark-utils/-/issues
.. _`new issue`: https://gitlab.aws.dev/aws-data-lab/bookmark-utils/-/issues/new
.. _Fork: https://docs.gitlab.com/ee/user/project/repository/forking_workflow.html
.. _`proper format`: https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html
.. _help: https://docs.gitlab.com/
.. _`Effective Guide to Pull Requests`: https://codeinthehole.com/writing/pull-requests-and-other-good-practices-for-teams-using-github/
.. _`Pull Request`: https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html
.. _mypy: https://mypy.readthedocs.io/
.. _cast: https://docs.python.org/3/library/typing.html#typing.cast
.. _`Amazon Open Source Code of Conduct`: https://aws.github.io/code-of-conduct
.. _`Code of Conduct FAQ`: https://aws.github.io/code-of-conduct-faq
.. _`vulnerability reporting page`: http://aws.amazon.com/security/vulnerability-reporting/
.. _LICENSE: ./LICENSE.txt
