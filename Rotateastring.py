def swap(reslist:list,st:int,ed:int):
    while st<ed:
        reslist[st],reslist[ed]=reslist[ed],reslist[st]
        st+=1
        ed-=1
def rotation(reslist:list,k:int)->str:
    slength=len(s)
    if(slength==0):
        return ''
    k%=slength
    swap(reslist,0,k-1)
    swap(reslist,k,slength-1)
    swap(reslist,0,slength-1)
    return ''.join(reslist)
if __name__=="__main__":
    s=input()
    k=int(input())
    reslist = list(s)
    print(rotation(reslist,k))

