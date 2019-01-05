#problem statemnt credits : http://stevekrenzel.com/finding-friends-with-mapreduce
from mrjob.job import MRJob
from mrjob.step import MRStep

class fb_common_friends(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.friend_tuple_former,
                   reducer=self.common_friends_finder)
        ]

    def friend_tuple_former(self, _, line):
        elements = line.split('->')
	user = elements[0]
	friends = elements[1].split(' ')
	
	for friend in friends:
	    friend_pair = self.utility_string_combiner(user.strip(),friend.strip())
	    if len(friend_pair.strip())>1:
	    	yield friend_pair, elements[1]

    def common_friends_finder(self, friend_tuple, friend_list):
	common_set = None
	for friends in friend_list:
	    #print friend_tuple,friends,common_set
	    if common_set == None:
		common_set = set(friends.strip().split(' '))
	    else:
		common_set = common_set.intersection(set(friends.split(' ')))
	
        print friend_tuple,len(list(common_set))

    #utilities ---------------------------------------------------
    def utility_string_combiner(self,A,B):
	if A<B:
	    return A+' '+B
	else:
	    return B+' '+A


if __name__ == '__main__':
    fb_common_friends.run()


#python fb_common_friends.py facebook_example.txt
